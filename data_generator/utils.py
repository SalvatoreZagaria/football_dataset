import datetime


def convert_to_date(text: str, quiet=True):
    def convert_from_iso():
        return datetime.date.fromisoformat(text)
    if quiet:
        try:
            return convert_from_iso()
        except:
            return None
    return convert_from_iso()
