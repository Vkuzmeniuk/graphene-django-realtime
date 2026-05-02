import graphene
from graphene import relay
from graphene_django import DjangoObjectType

from graphene_django_realtime import SubscriptionChannelRegistryMixin

from .models import Product


class ProductNode(DjangoObjectType):
    class Meta:
        model = Product
        interfaces = (relay.Node,)
        fields = ["name", "price"]


class Query(graphene.ObjectType):
    product = relay.Node.Field(ProductNode)


class Subscription(SubscriptionChannelRegistryMixin, graphene.ObjectType):
    product_updated = graphene.Field(ProductNode)

    @staticmethod
    def channel_product_updated(info, variables):
        return "products", variables.get("id", "all")


schema = graphene.Schema(query=Query, subscription=Subscription)
