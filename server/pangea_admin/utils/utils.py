import unidecode


def generate_safe_name(name):
    return unidecode.unidecode('{0}'.format(name)).replace(' ', '_').replace('-', '_').lower()

