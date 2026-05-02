"""Tests for GraphQLSerializer / serialize_for_broadcast."""

import pytest
from graphql_relay import from_global_id

from graphene_django_realtime import serialize_for_broadcast


@pytest.mark.django_db
class TestSerializeBasic:
    def setup_method(self):
        from tests.testapp.models import Product
        from tests.testapp.schema import ProductNode

        self.Product = Product
        self.ProductNode = ProductNode

    def test_returns_dict(self):
        p = self.Product.objects.create(name="Foo", price="1.00")
        result = serialize_for_broadcast(p, self.ProductNode)
        assert isinstance(result, dict)

    def test_scalar_fields_present(self):
        p = self.Product.objects.create(name="Bar", price="2.50")
        result = serialize_for_broadcast(p, self.ProductNode)
        assert result["name"] == "Bar"

    def test_id_is_relay_global_id(self):
        p = self.Product.objects.create(name="Baz", price="0.99")
        result = serialize_for_broadcast(p, self.ProductNode)
        type_name, raw_id = from_global_id(result["id"])
        assert type_name == "ProductNode"
        assert int(raw_id) == p.pk

    def test_typename_included(self):
        p = self.Product.objects.create(name="Qux", price="3.00")
        result = serialize_for_broadcast(p, self.ProductNode)
        assert result.get("__typename") == "ProductNode"


@pytest.mark.django_db
class TestContextPassedToResolvers:
    """
    Verifies that the context dict is available as info.context inside
    resolvers — the same filtering mechanism used by the HTTP API.
    """

    def test_context_is_accessible_via_custom_resolver(self):
        """
        A resolver that reads info.context should receive the context
        passed to serialize_for_broadcast.
        """
        import graphene
        from graphene import relay
        from graphene_django import DjangoObjectType
        from tests.testapp.models import Product

        class ProductWithCtxNode(DjangoObjectType):
            ctx_value = graphene.String()

            class Meta:
                model = Product
                interfaces = (relay.Node,)
                fields = ["name"]

            def resolve_ctx_value(self, info):
                return info.context.get("my_key", "missing")

        p = Product.objects.create(name="CtxTest", price="0")
        result = serialize_for_broadcast(
            p, ProductWithCtxNode, context={"my_key": "hello"}
        )
        assert result["ctxValue"] == "hello"

    def test_missing_context_key_returns_default(self):
        import graphene
        from graphene import relay
        from graphene_django import DjangoObjectType
        from tests.testapp.models import Product

        class ProductWithCtxNode(DjangoObjectType):
            ctx_value = graphene.String()

            class Meta:
                model = Product
                interfaces = (relay.Node,)
                fields = ["name"]

            def resolve_ctx_value(self, info):
                return info.context.get("my_key", "default")

        p = Product.objects.create(name="NoCtx", price="0")
        result = serialize_for_broadcast(p, ProductWithCtxNode)
        assert result["ctxValue"] == "default"


@pytest.mark.django_db
def test_serialize_decimal_field_is_string():
    from tests.testapp.models import Product
    from tests.testapp.schema import ProductNode

    p = Product.objects.create(name="Decimal", price="12.34")
    result = serialize_for_broadcast(p, ProductNode)
    assert result["price"] == "12.34"


def test_relay_id_roundtrip():
    from graphene_django_realtime._relay import from_global_id, to_global_id

    encoded = to_global_id("ProductNode", 42)
    result = from_global_id(encoded)
    # Verify both attribute access (.type/.id) and tuple unpacking work
    assert result.type == "ProductNode"
    assert result.id == "42"
    type_name, db_id = result
    assert type_name == "ProductNode"
    assert db_id == "42"


def test_relay_from_global_id_invalid():
    from graphene_django_realtime._relay import from_global_id

    with pytest.raises(ValueError, match="Invalid Relay global ID"):
        from_global_id("not_base64!!!")


def test_serializer_raises_on_incompatible_type():
    """Guard against future graphene-django versions that drop _meta.fields."""
    from graphene_django_realtime.serializers import GraphQLSerializer

    class NotADjangoObjectType:
        pass

    with pytest.raises(TypeError, match="_meta.fields"):
        GraphQLSerializer(NotADjangoObjectType)  # type: ignore[arg-type]
