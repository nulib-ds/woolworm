from typing import Tuple

import cv2
from loguru import logger
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
import numpy as np
import ollama
import pytesseract
from scipy.stats import entropy
from skimage.measure import shannon_entropy


class Woolworm:
    def __init__(
        self,
        paths: list | None = None,
        use_ollama: bool = False,
        use_hf: bool = False,
        transformer_model: str = "",
        benchmark: bool = False,
    ):
        self.paths = paths or []
        self.images = [path for path in self.paths]
        self.use_ollama = use_ollama
        self.use_hf = use_hf
        self.transformer_model = transformer_model
        self.results = None
        self.benchmark = benchmark

    @staticmethod
    def ocr(image_path: str, method: str = "tesseract", model: str = ""):
        """Run OCR (and optionally LLM) models on an image and returns a string of text.

        Features:
        - Using the method and model, customize Ollama or Marker Models.

        Args:
            image_path (str): Relative or absolute path to image
        Returns:
            str: Detected text in the image.
        """
        options = ["tesseract", "ollama", "marker"]
        if method.lower() not in options:
            logger.critical(
                f"{method} not found. Choose from 'ollama', 'tesseract' or 'huggingface'"
            )
            raise ValueError(f"Invalid OCR method: {method}")
        if method.lower() == "tesseract":
            return pytesseract.image_to_string(image_path)
        elif method.lower() == "marker":
            config = {"output_format": "html"}
            config_parser = ConfigParser(config)
            converter = PdfConverter(
                artifact_dict=create_model_dict(),
                config=config_parser.generate_config_dict(),
            )
            rendered = converter(image_path)
            text, _, images = text_from_rendered(rendered)
            return text
        elif method.lower() == "ollama":
            system_prompt = (
                "You are an OCR extraction assistant. "
                "Do not add any commentary, explanation, or extra text. "
                "Only output the exact text found in the image, formatted as requested (markdown tables, footnotes, headers). It is a matter of life or death that you do not repeat text."
            )
            prompt = "Extract the text from this image:\n\n"
            response = ollama.chat(
                model="gemma3:27b",
                options={
                    "seed": 42,
                    "temperature": 0.35,
                    "top_p": 0.95,
                    "top_k": 40,
                    "repetition_penalty": 50,
                },
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": prompt,
                        "images": [image_path],
                    },
                ],
            )
            return response["message"]["content"]

    @staticmethod
    def deskew_with_hough(img) -> np.ndarray:
        """Deskew an image containing text/diagrams using Hough + entropy check fallback.

        Features:
        - Uses entropy of a text-line mask to decide between Hough and projection profile.
        - Rejects phantom skew if detected angle is too small or too inconsistent.

        Args:
            img (np.ndarray): Input OpenCV image (BGR or grayscale).
        Returns:
            np.ndarray: Deskewed OpenCV image.
        """
        # --- Convert to grayscale if needed ---
        if len(img.shape) == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img.copy()

        # Invert so text = white
        gray_inv = cv2.bitwise_not(gray)

        # --- Morphological filtering to enhance horizontal text lines ---
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (30, 1))
        textline_mask = cv2.morphologyEx(gray_inv, cv2.MORPH_CLOSE, kernel)

        # --- Entropy of the mask (higher = more text-like structure) ---
        hist = cv2.calcHist([textline_mask], [0], None, [256], [0, 256])
        hist = hist.ravel() / hist.sum()
        mask_entropy = entropy(hist, base=2)

        # Threshold empirically: ~3.5 works well
        use_hough = mask_entropy > 3.5

        best_angle = 0
        angles = []

        if use_hough:
            # Edge detection for Hough
            edges = cv2.Canny(textline_mask, 50, 150, apertureSize=3)
            lines = cv2.HoughLines(edges, 1, np.pi / 180, 200)

            if lines is not None:
                for rho, theta in lines[:, 0]:
                    angle = (theta * 180 / np.pi) - 90
                    if -45 < angle < 45:  # only near-horizontal
                        angles.append(angle)

            if angles:
                best_angle = np.median(angles)
                # Consistency check: if angles too scattered, ignore
                if np.std(angles) > 5:
                    best_angle = 0
            else:
                use_hough = False  # fallback

        if not use_hough:
            # --- Projection profile fallback ---
            shift_range = np.arange(-15, 16)  # search ±15°
            scores = []
            for s in shift_range:
                M = cv2.getRotationMatrix2D(
                    (gray.shape[1] // 2, gray.shape[0] // 2), float(s), 1
                )
                rotated = cv2.warpAffine(
                    gray_inv,
                    M,
                    (gray.shape[1], gray.shape[0]),
                    flags=cv2.INTER_CUBIC,
                    borderMode=cv2.BORDER_REPLICATE,
                )
                proj = np.sum(rotated, axis=1)
                scores.append(np.var(proj))
            best_angle = shift_range[np.argmax(scores)]

        # --- Confidence threshold: skip tiny rotations ---
        if abs(best_angle) < 1.0:  # less than 1 degree
            best_angle = 0

        # --- Rotate original image if needed ---
        (h, w) = img.shape[:2]
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, float(best_angle), 1.0)
        rotated = cv2.warpAffine(
            img, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE
        )

        return rotated

    @staticmethod
    def binarize_or_gray(img, text_threshold=0.5, entropy_threshold=4.0, debug=False):
        """Detects if an image should be binarized or not, and if so, does that.

        Features:
        - Uses component counts to determine if the content of a page is a diagram or mostly text.
        - If predicted text, returns binarized.
        - If predicted diagram, returns copy of input image.

        Args:
            img (np.ndarray): Input OpenCV image (BGR or grayscale).
        Returns:
            np.ndarray: Deskewed OpenCV image.
        """
        gray = cv2.cvtColor(img, cv2.COLOR_RGB2GRAY)

        # Denoise
        denoised = cv2.fastNlMeansDenoising(
            gray, None, h=10, templateWindowSize=7, searchWindowSize=21
        )

        # --- Edge analysis for "textiness" ---
        edges = cv2.Canny(denoised, 50, 150)

        num_labels, labels, stats, _ = cv2.connectedComponentsWithStats(
            edges, connectivity=8
        )
        sizes = stats[1:, cv2.CC_STAT_AREA]  # skip background

        if len(sizes) == 0:
            logger.debug("No connected components found → returning grayscale")
            return gray, "diagram"

        small_components = np.sum(sizes < 300)
        ratio_small = small_components / (len(sizes) + 1e-5)

        # --- Entropy analysis ---
        entropy_val = shannon_entropy(edges)

        # --- Decision logic ---
        if len(sizes) < 2500:
            decision = "text"
            result = cv2.adaptiveThreshold(
                denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 35, 10
            )
        else:
            decision = "diagram"
            result = gray

        logger.debug(
            f"Decision={decision} | ratio_small={ratio_small:.3f} "
            f"(threshold={text_threshold}) | entropy={entropy_val:.3f} "
            f"(threshold={entropy_threshold}) | components={len(sizes)}"
        )

        return result

    @staticmethod
    def load(image_path):
        """Loads an arbitrary image from disk.

        Args:
            image_path (str or pathlike object): Path to image to load.
        Returns:
            np.ndarray: Numpy array representation of the image

        Examples:
            img = Woolworm().load("path/to/image.jpg")
        """
        return cv2.imread(image_path)

    @staticmethod
    def show(image):
        cv2.imshow("woolworm", image)
        cv2.waitKey(0)
        cv2.destroyAllWindows()

    @staticmethod
    def save_image(image, file_path):
        cv2.imwrite(file_path, image)
        return True

    @staticmethod
    def remove_borders(img):
        # Make a copy
        out = img.copy()

        # Create mask required by floodFill (2 pixels larger)
        h, w = out.shape[:2]
        mask = np.zeros((h + 2, w + 2), np.uint8)
        seeds: list[Tuple[int, int]] = [(0, 0), (w - 1, 0), (0, h - 1), (w - 1, h - 1)]
        # Flood fill from each corner (in case some sides aren't connected)
        for seed in seeds:
            seed: tuple[int, int]
            if out[seed[1], seed[0]] == 0:  # only flood if pixel is black
                cv2.floodFill(out, mask, seedPoint=seed, newVal=(255,))

        return out

    def save_ocr(self, output_dir: str, output_md: str = "output.md"):
        import os

        if self.results is None:
            raise RuntimeError("Run .infer() before .save()")
        os.makedirs(output_dir, exist_ok=True)
        md_path = os.path.join(output_dir, output_md)
        with open(md_path, "w", encoding="utf-8") as md_file:
            for idx, (orig, dns, bw_img, ocr_result) in enumerate(self.results):
                page_prefix = f"page_{idx + 1}"
                # Save images
                orig_path = os.path.join(output_dir, f"{page_prefix}_original.png")
                dns_path = os.path.join(output_dir, f"{page_prefix}_denoised.png")
                bw_path = os.path.join(output_dir, f"{page_prefix}_bw.png")
                if orig is not None:
                    cv2.imwrite(orig_path, orig)
                if dns is not None:
                    cv2.imwrite(dns_path, dns)
                if bw_img is not None:
                    cv2.imwrite(bw_path, bw_img)
                # Write markdown
                md_file.write(f"# Page {idx + 1}\n\n")
                md_file.write(f"![Original]({os.path.basename(orig_path)})\n\n")
                md_file.write(f"![Denoised]({os.path.basename(dns_path)})\n\n")
                md_file.write(f"![Black & White]({os.path.basename(bw_path)})\n\n")
                md_file.write(f"{ocr_result}\n\n")
        print(f"Saved markdown and images to {output_dir}")

    class Pipelines:
        def __init__(self, img):
            self.img = img

        @staticmethod
        def process_image(input_file_path, output_file_path):
            img = Woolworm.load(input_file_path)
            img = Woolworm.deskew_with_hough(img)
            img = Woolworm.binarize_or_gray(img)
            Woolworm.save_image(img, output_file_path)
            return img

        @staticmethod
        def ocr():
            pass
