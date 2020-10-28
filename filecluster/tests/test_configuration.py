from filecluster.configuration import get_default_config, Config


def test_get_default_config():
    config = get_default_config()
    assert isinstance(config, Config)
