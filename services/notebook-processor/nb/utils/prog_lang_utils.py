_cpp_config = {
    "file_extensions": ["cpp"],
    "jinja_prefix": "// j2",
    "jupytext_format": "cpp:percent",
    "language_info": {
        "codemirror_mode": "text/x-c++src",
        "file_extension": ".cpp",
        "mimetype": "text/x-c++src",
        "name": "c++",
        "version": "17",
    },
    "kernelspec": {"display_name": "C++17", "language": "C++17", "name": "xcpp17"},
}

_java_config = {
    "file_extensions": ["java"],
    "jinja_prefix": "// j2",
    "jupytext_format": "java:percent",
    "language_info": {
        "codemirror_mode": "java",
        "file_extension": ".java",
        "mimetype": "text/java",
        "name": "Java",
        "pygments_lexer": "java",
        "version": "",
    },
    "kernelspec": {"display_name": "Java", "language": "java", "name": "java"},
}

_python_config = {
    "file_extensions": ["py"],
    "jinja_prefix": "# j2",
    "jupytext_format": "py:percent",
    "language_info": {
        "codemirror_mode": {"name": "ipython", "version": 3},
        "file_extension": ".py",
        "mimetype": "text/x-python",
        "name": "python",
        "nbconvert_exporter": "python",
        "pygments_lexer": "ipython3",
    },
    "kernelspec": {
        "display_name": "Python 3 (ipykernel)",
        "language": "python",
        "name": "python3",
    },
}

_rust_config = {
    "file_extensions": ["rs"],
    "jinja_prefix": "# j2",
    "jupytext_format": "md",
    "language_info": {
        "codemirror_mode": "rust",
        "file_extension": ".rs",
        "mimetype": "text/rust",
        "name": "Rust",
        "pygment_lexer": "rust",
        "version": "",
    },
    "kernelspec": {"display_name": "Rust", "language": "rust", "name": "rust"},
}


class Config:
    def __init__(self, cpp, java, python, rust):
        self.prog_lang = {
            "cpp": cpp,
            "java": java,
            "python": python,
            "rust": rust,
        }


config = Config(
    cpp=_cpp_config, java=_java_config, python=_python_config, rust=_rust_config
)


def suffix_for(prog_lang: str) -> str:
    try:
        return "." + config.prog_lang[prog_lang]["file_extensions"][0]
    except KeyError:
        raise ValueError(f"Unsupported language: {prog_lang}")


def jupytext_format_for(prog_lang: str) -> str:
    try:
        return config.prog_lang[prog_lang]["jupytext_format"]
    except KeyError:
        raise ValueError(f"Unsupported language: {prog_lang}")


def language_info(prog_lang: str) -> dict:
    try:
        return config.prog_lang[prog_lang]["language_info"]
    except KeyError:
        raise ValueError(f"Unsupported language: {prog_lang}")


def file_extension_for(prog_lang: str) -> str:
    try:
        return language_info(prog_lang)["file_extension"]
    except KeyError:
        raise ValueError(f"Unsupported language: {prog_lang}")


def kernelspec_for(prog_lang: str) -> dict:
    try:
        return config.prog_lang[prog_lang]["kernelspec"]
    except KeyError:
        raise ValueError(f"Unsupported language: {prog_lang}")
