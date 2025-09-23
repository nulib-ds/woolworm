import typer
from typing_extensions import Annotated


def greet(
    name: Annotated[
        str,
        typer.Argument(
            help="The (last, if --title is given) name of the person to greet"
        ),
    ] = "",
    title: Annotated[
        str, typer.Option(help="The preferred title of the person to greet")
    ] = "",
    doctor: Annotated[
        bool, typer.Option(help="Whether the person is a doctor (MD or PhD)")
    ] = False,
    count: Annotated[int, typer.Option(help="Number of times to greet the person")] = 1,
):
    greeting = "Greetings, "
    if doctor and not title:
        title = "Dr."
    if not name:
        if title:
            name = title.lower().rstrip(".")
        else:
            name = "friend"
    if title:
        greeting += f"{title} "
    greeting += f"{name}!"
    for i in range(0, count):
        print(greeting)
