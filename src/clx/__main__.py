
import click


def say_hi(name="world"):
    click.echo(f"Hello, {name}!")


@click.command()
@click.option("--name", help="The name of the person to greet.", default="world")
def main(name):
    say_hi(name)


if __name__ == "__main__":
    main()


