# coding: utf-8
from hashlib import md5
from re import sub
from html.parser import HTMLParser


def md5_long(data):
    """
    https://stackoverflow.com/a/3735845
    According to this discussion and previous experience,
    most significant bytes of md5(url) suit well as primary key.
    However, postgresql does not allow to store ui64 in its bigint type:
    https://stackoverflow.com/a/21910350
    So I have just left 7 bytes instead of 8 to make it fit into bigint.
    >>> md5_long(b'some text')
    23976095733569937
    """
    md5hash = md5(data).digest()
    i64_val = 0
    for i in range(7):
        i64_val = (i64_val << 8) | (md5hash[i] & 0xFF)
    return i64_val


# based on https://stackoverflow.com/a/3987802
class HtmlDataParser(HTMLParser):
    """
    Extract text from tags. Skip content of <script> and <style>.
    >>> parser = HtmlDataParser()
    >>> parser.feed('<head>Some head text</head><script>some script</script>' \
                    '<body>Body contains this:   abrakadabra</body>')
    >>> parser.close()
    >>> parser.text()
    'Some head text Body contains this: abrakadabra'
    """
    def __init__(self):
        HTMLParser.__init__(self)
        self.__text = []
        self._skip_next_data = False

    def handle_data(self, data):
        text = data.strip()
        if not self._skip_next_data and len(text) > 0:
            text = sub('[ \t\r\n]+', ' ', text)
            self.__text.append(text + ' ')

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self._skip_next_data = True

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self._skip_next_data = False

    def text(self):
        return ''.join(self.__text).strip()


def unique_by_nth_field(items, key_idx=0, keep_last=True):
    """
    return elements of iterable unique by each element's key_idx field.
    if keep_last, store last element by every key
    >>> items = [(1, 10, 100), (2, 11, 111), (1, 11, 100)]
    >>> list(unique_by_nth_field(items, key_idx=0, keep_last=True))
    [(1, 11, 100), (2, 11, 111)]
    >>> list(unique_by_nth_field(items, key_idx=1, keep_last=False))
    [(1, 10, 100), (2, 11, 111)]
    """
    seen = dict()
    for item in items:
        key = item[key_idx]
        if key not in seen or keep_last:
            seen[key] = item
    return seen.values()
