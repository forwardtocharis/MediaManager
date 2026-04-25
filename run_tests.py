import unittest
import sys

def run():
    print("\n" + "="*60)
    print("  MediaManager - Automated Test Suite")
    print("="*60 + "\n")

    loader = unittest.TestLoader()
    suite = loader.discover('tests', pattern='test_*.py')

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "="*60)
    if result.wasSuccessful():
        print("  [PASS] ALL TESTS PASSED")
    else:
        print(f"  [FAIL] TESTS FAILED: {len(result.failures)} failures, {len(result.errors)} errors")
    print("="*60 + "\n")

    sys.exit(0 if result.wasSuccessful() else 1)

if __name__ == "__main__":
    run()
