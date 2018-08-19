def model_name(model):
    import re

    s = str(type(model))
    model_name = re.search(".*'(.+?)'.*", s).group(1).split(".")[-1]
    return model_name
