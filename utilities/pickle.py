from pickle import dump, load


def pickle_dump(obj, file_name: str):
    with open(file_name, 'wb') as fio:
        dump(object, fio, protocol=4)

    return obj


def pickle_load(file_name: str) -> object:
    with open(file_name, 'rb') as fio:
        obj = load(fio)

    return obj
