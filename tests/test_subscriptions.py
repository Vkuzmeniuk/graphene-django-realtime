"""Tests for SubscriptionChannelRegistryMixin and the channel registry."""

import graphene
import pytest

from graphene_django_realtime import SubscriptionChannelRegistryMixin
from graphene_django_realtime.subscriptions import (
    clear_subscription_channels,
    get_subscription_channel,
    register_subscription_channel,
)


class _Info:
    context = {}


# All tests in this module get an isolated (empty) registry so dynamically
# defined Subscription classes don't leak between tests.
pytestmark = pytest.mark.usefixtures("isolated_registry")


# ---------------------------------------------------------------------------
# Auto-registration via __init_subclass__
# ---------------------------------------------------------------------------


def test_channel_method_auto_registered():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        my_field = graphene.String()

        @staticmethod
        def channel_my_field(info, variables):
            return "things", variables.get("id", "all")

    result = get_subscription_channel("myField", _Info(), {"id": "42"})
    assert result == ("things", "42")


def test_snake_to_camel_conversion():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        categories_by_budget = graphene.String()

        @staticmethod
        def channel_categories_by_budget(info, variables):
            return "budget", variables["idbudget"]

    result = get_subscription_channel("categoriesByBudget", _Info(), {"idbudget": "7"})
    assert result == ("budget", "7")


def test_three_tuple_with_filters():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        items = graphene.String()

        @staticmethod
        def channel_items(info, variables):
            return "items", "all", {
                "start": variables.get("start"),
                "end": variables.get("end"),
            }

    result = get_subscription_channel(
        "items", _Info(), {"start": "2026-01-01", "end": "2026-12-31"}
    )
    assert result == ("items", "all", {"start": "2026-01-01", "end": "2026-12-31"})


def test_class_without_channel_methods_does_not_register():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        plain_field = graphene.String()

    with pytest.raises(KeyError):
        get_subscription_channel("plainField", _Info(), {})


def test_multiple_channel_methods_all_registered():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        field_a = graphene.String()
        field_b = graphene.String()

        @staticmethod
        def channel_field_a(info, variables):
            return "ch_a", "1"

        @staticmethod
        def channel_field_b(info, variables):
            return "ch_b", "2"

    assert get_subscription_channel("fieldA", _Info(), {}) == ("ch_a", "1")
    assert get_subscription_channel("fieldB", _Info(), {}) == ("ch_b", "2")


def test_non_callable_attribute_with_channel_prefix_is_ignored():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        pass

    Subscription.channel_not_callable = "just a string"

    with pytest.raises(KeyError):
        get_subscription_channel("notCallable", _Info(), {})


# ---------------------------------------------------------------------------
# Explicit registration
# ---------------------------------------------------------------------------


def test_explicit_registration():
    register_subscription_channel("explicitField", lambda i, v: ("ch", v.get("id")))
    result = get_subscription_channel("explicitField", _Info(), {"id": "99"})
    assert result == ("ch", "99")


def test_explicit_registration_overwrites_channel_method():
    class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
        my_thing = graphene.String()

        @staticmethod
        def channel_my_thing(info, variables):
            return "original", "1"

    register_subscription_channel("myThing", lambda i, v: ("overwritten", "2"))
    result = get_subscription_channel("myThing", _Info(), {})
    assert result == ("overwritten", "2")


# ---------------------------------------------------------------------------
# clear_subscription_channels
# ---------------------------------------------------------------------------


def test_clear_removes_all_registrations():
    register_subscription_channel("alpha", lambda i, v: ("a", "1"))
    register_subscription_channel("beta", lambda i, v: ("b", "2"))
    clear_subscription_channels()
    with pytest.raises(KeyError):
        get_subscription_channel("alpha", _Info(), {})
    with pytest.raises(KeyError):
        get_subscription_channel("beta", _Info(), {})
