"""Basic tests for the project skeleton."""


def test_package_imports() -> None:
    """The package should be importable after installation."""
    import market_sentinel

    assert market_sentinel.__version__ == "0.1.0"
