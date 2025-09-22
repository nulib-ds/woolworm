import os
import argparse
import subprocess

# This file acts as a supercomputing wrapper for Woolworm.


def main():
    parser = argparse.ArgumentParser(description="Batch submit woolworm jobs")
    parser.add_argument("account", help="SLURM account (e.g., p12345)")
    parser.add_argument("email", help="email for SLURM notifications")
    parser.add_argument("parent_dir", help="Parent directory containing subdirectories")
    parser.add_argument(
        "--output_root", default="outputs", help="Root output directory"
    )
    parser.add_argument("--extra_args", default="", help="Extra args for woolworm")
    args = parser.parse_args()

    if not os.path.exists(args.parent_dir):
        raise FileNotFoundError(f"Parent directory {args.parent_dir} does not exist.")

    os.makedirs(args.output_root, exist_ok=True)

    for name in os.listdir(args.parent_dir):
        subdir = os.path.join(args.parent_dir, name)
        print(name, subdir)
        if os.path.isdir(subdir):
            output_dir = os.path.join(args.output_root, name)
            os.makedirs(output_dir, exist_ok=True)

            jp2_count = sum(
                len([f for f in files if f.endswith(".jp2")])
                for _, _, files in os.walk(subdir)
            )
            total_seconds = jp2_count * 10
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            slurm_time = f"{hours}:{minutes:02}:{seconds:02}"
            if hours > 4:
                partition = "normal"
            else:
                partition = "short"
            # Use raw f-string to preserve bash $ and { } syntax
            SLURM_TEMPLATE = rf"""#!/bin/bash
#SBATCH --account={args.account}
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=image_processing
#SBATCH --time={slurm_time}
#SBATCH --mem=16GB
#SBATCH --output=output-%j.out
#SBATCH --error=error-%j.err
#SBATCH --mail-type=ALL
#SBATCH --mail-user={args.email}

uv run ./main.py -i {subdir} -o ./{output_dir} {args.extra_args} --recursive
"""

            script_path = f"submit_{name}.sh"
            with open(script_path, "w") as f:
                f.write(SLURM_TEMPLATE)

            subprocess.run(["sbatch", script_path])
            os.remove(script_path)


if __name__ == "__main__":
    main()
