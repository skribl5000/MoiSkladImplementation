
def test_import():
    try:
        import pywb
        result = True

    except ImportError:
        result = False

    assert result
