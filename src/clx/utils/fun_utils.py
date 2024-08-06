def arg(i: int, name: str, args: list, kwargs: dict):
    token = object()
    pos_arg = args[i] if i < len(args) else token
    kw_arg = kwargs.get(name, token)

    if pos_arg != token and kw_arg != token:
        if pos_arg == kw_arg:
            return pos_arg
        else:
            raise ValueError(f"Argument {name} has conflicting values: "
                             f"{pos_arg} and {kw_arg}")
    elif pos_arg != token:
        return pos_arg
    elif kw_arg != token:
        return kw_arg
    else:
        raise ValueError(f"Argument {name} is missing")


