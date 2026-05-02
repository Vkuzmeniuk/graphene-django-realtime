import pytest


@pytest.fixture()
def isolated_registry():
    """
    Snapshot the subscription registry before the test and restore it after.
    Needed in test_subscriptions.py to prevent pollution from dynamically
    defined Subscription classes.
    """
    from graphene_django_realtime import subscriptions as _subs

    saved = dict(_subs._REGISTRY)
    _subs._REGISTRY.clear()
    yield
    _subs._REGISTRY.clear()
    _subs._REGISTRY.update(saved)
