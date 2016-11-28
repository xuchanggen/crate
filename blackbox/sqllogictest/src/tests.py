import unittest
import os
import re
import faulthandler
import pathlib
from functools import partial
from testutils.ports import GLOBAL_PORT_POOL
from testutils.paths import crate_path, project_root
from crate.testing.layer import CrateLayer
from sqllogictest import run_file

CRATE_HTTP_PORT = GLOBAL_PORT_POOL.get()
CRATE_TRANSPORT_PORT = GLOBAL_PORT_POOL.get()

tests_path = pathlib.Path(os.path.abspath(os.path.join(
    project_root, 'blackbox', 'sqllogictest', 'testfiles', 'test')))

# Enable to be able to dump threads in case something gets stuck
faulthandler.enable()

# might want to change this to a blacklist at some point
whitelist = set([
    'select\d+.test',
    'random/select/slt_good_\d+.test',
])


class TestMaker(type):

    def __new__(cls, name, bases, attrs):
        patterns = [re.compile(item) for item in whitelist]
        for filename in tests_path.glob('**/*.test'):
            filepath = tests_path / filename
            relpath = str(filepath.relative_to(tests_path))
            if not True in [pattern.match(str(relpath)) and True or False for pattern in patterns]:
                continue
            attrs['test_' + relpath] = partial(
                run_file,
                fh=filepath.open('r', encoding='utf-8'),
                hosts='localhost:' + str(CRATE_HTTP_PORT),
                verbose=True,
                failfast=False
            )
        return type.__new__(cls, name, bases, attrs)


class SqlLogicTest(unittest.TestCase, metaclass=TestMaker):
    pass


def test_suite():
    suite = unittest.TestSuite(unittest.makeSuite(SqlLogicTest))
    crate_layer = CrateLayer(
        'crate-sqllogic',
        crate_home=crate_path(),
        port=CRATE_HTTP_PORT,
        transport_port=CRATE_TRANSPORT_PORT,
        settings={
            'stats.enabled': True
        }
    )
    suite.layer = crate_layer
    return suite
