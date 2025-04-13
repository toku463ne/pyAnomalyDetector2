import __init__

def run_tests():
    """
    run all tests
    """
    import unittest
    import tests.test_csv_getter
    import tests.test_config_loader
    import tests.test_normalizer
    import tests.test_postgresql
    import tests.test_history_model
    import tests.test_anomalies_model
    import tests.test_trends_stats_model
    import tests.test_flask_view
    import tests.test_trends_stats

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    suite.addTests(loader.loadTestsFromModule(tests.test_csv_getter))
    suite.addTests(loader.loadTestsFromModule(tests.test_config_loader))
    suite.addTests(loader.loadTestsFromModule(tests.test_normalizer))
    suite.addTests(loader.loadTestsFromModule(tests.test_postgresql))
    suite.addTests(loader.loadTestsFromModule(tests.test_history_model))
    suite.addTests(loader.loadTestsFromModule(tests.test_anomalies_model))
    suite.addTests(loader.loadTestsFromModule(tests.test_trends_stats_model))
    suite.addTests(loader.loadTestsFromModule(tests.test_trends_stats))
    #suite.addTests(loader.loadTestsFromModule(tests.test_flask_view))
    

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

if __name__ == '__main__':
    run_tests()