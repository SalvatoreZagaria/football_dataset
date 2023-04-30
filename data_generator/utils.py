import datetime


def convert_to_date(text: str, quiet=True):
    def convert():
        if len(text) == 6:
            return datetime.date(day=int(text[:2]), month=int(text[2:4]), year=int(f'20{text[4:]}'))
        return datetime.date.fromisoformat(text)
    if quiet:
        try:
            return convert()
        except:
            return None
    return convert()
