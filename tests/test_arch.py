import time
import unittest
from kubetk.arch.pipeline import pipeline_run


class TestPipeline(unittest.TestCase):

    def test_pipeline_math(self):
        a = []
        pipeline_run(range(20), [
            ((lambda x: time.sleep(0.2) or x + 1), 4),
            ((lambda x: x * x), 2),
            ((lambda x: a.append(x)), 1)
        ])
        self.assertSetEqual(set(a), set((x + 1) ** 2 for x in range(20)))

    def error_pipeline(self):
        pipeline_run(range(20), [
            ((lambda x: x + 1), 4),
            ((lambda x: None * x), 2),
            ((lambda x: x + 1), 1)
        ])

    def test_pipeline_error(self):
        self.assertRaises(TypeError, self.error_pipeline)


if __name__ == '__main__':
    unittest.main()
