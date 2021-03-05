import doctest
import importlib
import itertools
import pkgutil
import unittest

import util
import wsc_consumer
import wsc_producer


def _load_package_tests(package_path, package_name):
    for _, module, _ in pkgutil.walk_packages(path=package_path, prefix=package_name + '.'):
        yield doctest.DocTestSuite(module=importlib.import_module(module))


def load_tests(_loader, tests, _ignore):
    for test in itertools.chain(
            _load_package_tests(util.__path__, util.__name__),
            _load_package_tests(wsc_consumer.__path__, wsc_consumer.__name__),
            _load_package_tests(wsc_producer.__path__, wsc_producer.__name__),
    ):
        tests.addTest(test)
    return tests


class TestHtmlDataParser(unittest.TestCase):
    def test_parser(self):
        doc_body = '<head>Some head text</head><script>some script</script>' \
                   '<body>Body contains this:   abrakadabra</body>'
        expected = 'Some head text Body contains this: abrakadabra'
        parser = util.utils.HtmlDataParser()
        parser.feed(doc_body)
        parser.close()
        doc_text = parser.text()
        self.assertEqual(doc_text, expected)


if __name__ == '__main__':
    unittest.main()
