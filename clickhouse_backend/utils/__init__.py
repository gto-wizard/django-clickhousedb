def get_subclasses(class_):
    classes = class_.__subclasses__()
    index = 0
    while index < len(classes):
        classes.extend(classes[index].__subclasses__())
        index += 1
    return list(set(classes))
