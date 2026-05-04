"""
GraphQL-based serializers for WebSocket broadcasting.

Reuses GraphQL schema resolvers to generate payloads without executing queries.
Ensures consistency between HTTP API and WebSocket messages.
"""
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, Any, Optional, List, Type
from graphene_django.types import DjangoObjectType
from graphene import relay
from graphene.utils.str_converters import to_camel_case, to_snake_case
from django.db.models import Model, QuerySet, Manager
from ._relay import to_global_id
import logging

logger = logging.getLogger(__name__)

_MODEL_TO_GRAPHQL_TYPE_REGISTRY = {}
_SKIP_FIELDS = frozenset({"_meta", "pk"})


def _is_filter_connection_field(field_def: object) -> bool:
    """
    Duck-typed replacement for ``isinstance(field_def, DjangoFilterConnectionField)``.
    Works regardless of whether django-filter is installed or graphene-django
    renames/moves the class in a future version.
    """
    return "ConnectionField" in type(field_def).__name__


def register_model_type(model_class: Type[Model], graphql_type: Type[DjangoObjectType]):
    """Registers Django Model → GraphQL Type mapping for fallback type detection."""
    model_name = model_class.__name__
    _MODEL_TO_GRAPHQL_TYPE_REGISTRY[model_name] = graphql_type
    logger.debug(f"Registered model type: {model_name} → {graphql_type.__name__}")


def get_graphql_type_for_model(model_instance: Model) -> Optional[Type[DjangoObjectType]]:
    """Returns GraphQL type for Django model instance."""
    model_name = model_instance.__class__.__name__
    return _MODEL_TO_GRAPHQL_TYPE_REGISTRY.get(model_name)


_discovery_done = False


def auto_discover_types():
    """
    Auto-discovers and registers all GraphQL types from *schema.py files.

    Called lazily on the first invocation of serialize_for_broadcast() so that
    all Django apps and their schema modules are fully loaded before discovery
    runs. Safe to call manually at any point after Django startup.
    """
    global _discovery_done
    import importlib
    from django.apps import apps

    registered_count = 0

    for app_config in apps.get_app_configs():
        if app_config.name.startswith('django.'):
            continue

        try:
            schema_module = importlib.import_module(f'{app_config.name}.schema')

            for attr_name in dir(schema_module):
                attr = getattr(schema_module, attr_name)

                if (isinstance(attr, type) and
                    issubclass(attr, DjangoObjectType) and
                    attr is not DjangoObjectType):

                    if hasattr(attr, '_meta') and hasattr(attr._meta, 'model'):
                        model_class = attr._meta.model

                        if model_class.__name__ not in _MODEL_TO_GRAPHQL_TYPE_REGISTRY:
                            register_model_type(model_class, attr)
                            registered_count += 1

        except ImportError:
            continue
        except Exception as e:
            logger.warning(f"Error discovering types in {app_config.name}.schema: {e}")

    _discovery_done = True
    logger.info(f"Auto-discovered and registered {registered_count} GraphQL types")


def _ensure_discovery():
    """Run auto_discover_types() once, lazily. Safe to call repeatedly."""
    if _discovery_done:
        return
    try:
        auto_discover_types()
    except Exception as e:
        logger.debug(f"Deferred auto_discover_types(): {e}")


class MockInfo:
    """
    Mock `info` object for passing context to GraphQL resolvers.

    Resolvers commonly read `info.context`. Less commonly they touch
    `info.field_name` / `info.path` / `info.schema` / `info.return_type` /
    `info.operation` — those are populated as `None` so a lookup doesn't
    raise `AttributeError` even when an unusual resolver runs against this stub.
    """

    __slots__ = (
        "context",
        "field_name",
        "parent_type",
        "return_type",
        "path",
        "schema",
        "fragments",
        "root_value",
        "operation",
        "variable_values",
    )

    def __init__(self, context: Optional[Dict[str, Any]] = None):
        self.context = context or {}
        self.field_name = None
        self.parent_type = None
        self.return_type = None
        self.path = None
        self.schema = None
        self.fragments = {}
        self.root_value = None
        self.operation = None
        self.variable_values = {}


class GraphQLSerializer:
    """
    Walks a ``DjangoObjectType``'s fields, calls the same resolvers the GraphQL
    executor would, and returns a JSON-serializable dict.

    Prefer the :func:`serialize_for_broadcast` helper over instantiating this
    class directly.
    """

    max_depth: int = 3
    """Maximum nested-relation depth before falling back to a stub `{id, __typename}`."""

    def __init__(self, graphql_type: Type[DjangoObjectType]):
        if not (hasattr(graphql_type, "_meta") and hasattr(graphql_type._meta, "fields")):
            raise TypeError(
                f"{graphql_type!r} has no _meta.fields — ensure it is a DjangoObjectType "
                "subclass and that your graphene-django version is compatible."
            )
        self.graphql_type = graphql_type
        self.model = graphql_type._meta.model
        self.type_name = graphql_type.__name__

    def serialize(
        self,
        instance: Model,
        context: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        _depth: int = 0,
        _visited: Optional[set] = None
    ) -> Dict[str, Any]:
        """
        Serializes Django model to GraphQL payload.

        Args:
            instance: Django model instance
            context: Context dictionary with filters
            fields: List of fields to serialize (None = all)
        """
        if instance is None:
            return None

        if _depth >= self.max_depth:
            return {"id": to_global_id(self.type_name, instance.pk), "__typename": self.type_name}

        if _visited is None:
            _visited = set()

        obj_key = (self.type_name, instance.pk)
        if obj_key in _visited:
            return {"id": to_global_id(self.type_name, instance.pk), "__typename": self.type_name}

        _visited.add(obj_key)

        info = MockInfo(context=context or {})
        payload = {"__typename": self.type_name}

        graphql_fields = self.graphql_type._meta.fields
        fields_set = {*fields, *(to_snake_case(f) for f in fields)} if fields else None

        for field_name, field_def in graphql_fields.items():
            if fields_set is not None and field_name not in fields_set:
                continue

            if field_name in _SKIP_FIELDS:
                continue

            # Skip auto-generated reverse relations
            if field_name.endswith('_set') and not _is_filter_connection_field(field_def):
                continue

            try:
                value = self._resolve_field(instance, field_name, field_def, info)
                converted = self._convert_value(value, field_def, info, _depth, _visited)
                final_key = to_camel_case(field_name) if not field_name.startswith('__') else field_name
                payload[final_key] = converted

            except Exception as e:
                logger.error(f"Failed to serialize field '{field_name}' of {self.type_name}: {e}", exc_info=True)
                final_key = to_camel_case(field_name) if not field_name.startswith('__') else field_name
                payload[final_key] = None

        # Add Global ID for Relay nodes
        if hasattr(self.graphql_type._meta, 'interfaces'):
            if relay.Node in self.graphql_type._meta.interfaces:
                payload['id'] = to_global_id(self.type_name, instance.pk)

        return payload

    def _resolve_field(
        self,
        instance: Model,
        field_name: str,
        field_def: Any,
        info: MockInfo
    ) -> Any:
        """
        Gets field value via resolver or model attribute.

        Priority:
        1. Custom resolve_* method in serializer
        2. Custom resolve_* method in GraphQL type
        3. Model attribute
        """
        info.field_name = field_name

        # Check serializer custom resolver
        serializer_resolver_name = f"resolve_{field_name}"
        if hasattr(self, serializer_resolver_name):
            resolver = getattr(self, serializer_resolver_name)
            return resolver(instance, info)

        # Check GraphQL type resolver
        resolver_name = f"resolve_{field_name}"
        if hasattr(self.graphql_type, resolver_name):
            resolver = getattr(self.graphql_type, resolver_name)
            return resolver(instance, info)

        # Get model attribute
        if hasattr(instance, field_name):
            value = getattr(instance, field_name)
        else:
            snake_name = to_snake_case(field_name)
            value = getattr(instance, snake_name, None)

        # Handle RelatedManager/QuerySet
        if isinstance(value, (Manager, QuerySet)):
            if hasattr(value, "all"):
                return value.all()

        return value

    def _convert_value(
        self,
        value: Any,
        field_def: Any,
        info: MockInfo,
        _depth: int = 0,
        _visited: Optional[set] = None
    ) -> Any:
        """Converts value to JSON-compatible format."""
        if value is None:
            return None

        if isinstance(value, Decimal):
            return str(value)

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if isinstance(value, (QuerySet, list)):
            return self._serialize_queryset(value, field_def, info, _depth, _visited)

        if isinstance(value, Model):
            return self._serialize_related(value, field_def, info, _depth, _visited)

        return value

    def _serialize_queryset(
        self,
        queryset: QuerySet,
        field_def: Any,
        info: MockInfo,
        _depth: int = 0,
        _visited: Optional[set] = None
    ) -> Dict[str, Any]:
        """Serializes QuerySet to Relay Connection format."""
        def _is_node_type(t: Any) -> bool:
            return (
                isinstance(t, type)
                and issubclass(t, DjangoObjectType)
                and hasattr(t, "_meta")
                and hasattr(t._meta, "model")
            )

        inner_type = self._get_inner_type(field_def)

        # Handle Graphene Connection types by extracting their node type
        if inner_type and hasattr(inner_type, "_meta") and not hasattr(inner_type._meta, "model"):
            node_type = getattr(inner_type._meta, "node", None) or getattr(inner_type._meta, "node_type", None)
            if callable(node_type):
                try:
                    node_type = node_type()
                except Exception:
                    node_type = None
            if hasattr(node_type, "of_type"):
                node_type = node_type.of_type
            if _is_node_type(node_type):
                inner_type = node_type

        # Fallback: determine type from first QuerySet element
        if not _is_node_type(inner_type) and queryset:
            first_item = queryset.first() if hasattr(queryset, 'first') else (queryset[0] if queryset else None)
            if first_item:
                inner_type = get_graphql_type_for_model(first_item)

        if _is_node_type(inner_type):
            edge_serializer = GraphQLSerializer(inner_type)

            edges = [
                {
                    "node": edge_serializer.serialize(
                        item,
                        context=info.context,
                        _depth=_depth + 1,
                        _visited=_visited
                    ),
                    "__typename": f"{inner_type.__name__}Edge",
                }
                for item in queryset
            ]

            return {
                "edges": edges,
                "__typename": f"{inner_type.__name__}Connection",
            }

        # Return empty connection if type cannot be determined
        return {
            "edges": [],
            "__typename": "Connection",
        }

    def _serialize_related(
        self,
        instance: Model,
        field_def: Any,
        info: MockInfo,
        _depth: int = 0,
        _visited: Optional[set] = None
    ) -> Dict[str, Any]:
        """Serializes related model (ForeignKey, OneToOne)."""
        related_type = self._get_inner_type(field_def)

        if related_type and isinstance(related_type, type) and issubclass(related_type, DjangoObjectType):
            related_serializer = GraphQLSerializer(related_type)
            return related_serializer.serialize(
                instance,
                context=info.context,
                _depth=_depth + 1,
                _visited=_visited
            )

        # Fallback: try finding type via registry
        fallback_type = get_graphql_type_for_model(instance)
        if fallback_type:
            related_serializer = GraphQLSerializer(fallback_type)
            return related_serializer.serialize(
                instance,
                context=info.context,
                _depth=_depth + 1,
                _visited=_visited
            )

        return {"id": to_global_id("Node", instance.pk)}

    def _get_inner_type(self, field_def: Any) -> Optional[Type]:
        """Extracts inner type from Field/Connection/List."""

        def _unwrap_type(field_type: Any) -> Any:
            if hasattr(field_type, "of_type"):
                return _unwrap_type(field_type.of_type)
            return field_type

        def _coerce_node_type(node_type: Any) -> Optional[Type[DjangoObjectType]]:
            if callable(node_type):
                try:
                    node_type = node_type()
                except Exception:
                    return None
            node_type = _unwrap_type(node_type)
            if node_type and isinstance(node_type, type) and issubclass(node_type, DjangoObjectType):
                return node_type
            return None

        if _is_filter_connection_field(field_def):
            # Try extracting via _type
            if hasattr(field_def, '_type'):
                node_type = field_def._type
                node_type = _coerce_node_type(node_type)

                if node_type:
                    return node_type

            # Try via node_type attribute
            if hasattr(field_def, 'node_type'):
                node_type = _coerce_node_type(field_def.node_type)
                if node_type:
                    return node_type

        # Regular fields
        if hasattr(field_def, 'type'):
            field_type = _unwrap_type(field_def.type)
            if callable(field_type):
                try:
                    field_type = field_type()
                except Exception:
                    pass

            # Extract node from Connection via meta.node or meta.node_type
            if hasattr(field_type, "_meta"):
                node_type = getattr(field_type._meta, "node", None) or getattr(field_type._meta, "node_type", None)
                node_type = _coerce_node_type(node_type)
                if node_type:
                    return node_type

            # Extract node from Connection via Edge
            if hasattr(field_type, 'Edge'):
                try:
                    edge_type = field_type.Edge
                    if hasattr(edge_type, 'node'):
                        node_field = edge_type.node
                        if hasattr(node_field, 'type'):
                            res_type = _coerce_node_type(node_field.type)
                            if res_type:
                                return res_type
                except Exception:
                    pass

            # Unwrap List/NonNull
            if hasattr(field_type, 'of_type'):
                return self._get_inner_type(field_type)

            if isinstance(field_type, type) and issubclass(field_type, DjangoObjectType):
                return field_type

            return field_type

        if hasattr(field_def, 'of_type'):
            return self._get_inner_type(field_def.of_type)

        return None


class FilterableSerializer(GraphQLSerializer):
    """
    Serializer with automatic context-based filtering.

    Automatically uses resolvers from GraphQL schema that support filtering via info.context.
    """
    pass


class SerializerFactory:
    """Factory for creating and caching serializer instances."""

    _cache: Dict[Type[DjangoObjectType], GraphQLSerializer] = {}

    @classmethod
    def get(
        cls,
        graphql_type: Type[DjangoObjectType],
        serializer_class: Type[GraphQLSerializer] = FilterableSerializer
    ) -> GraphQLSerializer:
        """Gets or creates cached serializer instance."""
        cache_key = (graphql_type, serializer_class)

        if cache_key not in cls._cache:
            cls._cache[cache_key] = serializer_class(graphql_type)

        return cls._cache[cache_key]


def serialize_for_broadcast(
    model_instance: Model,
    graphql_type: Type[DjangoObjectType],
    context: Optional[Dict[str, Any]] = None,
    serializer_class: Type[GraphQLSerializer] = FilterableSerializer
) -> Dict[str, Any]:
    """
    Universal function for serializing models for WebSocket broadcast.

    This is a PURE SERIALIZATION function - it does NOT send anything.

    Example:
        from graphene_django_realtime import serialize_for_broadcast
        from expense.schema import ExpenseCategoryNode

        payload = serialize_for_broadcast(
            category_instance,
            ExpenseCategoryNode,
            context={'start_date': '2024-01-01', 'end_date': '2024-12-31'}
        )

        # Push directly to a single socket via the consumer's built-in
        # ``graphql_event`` handler.  ``type`` MUST be ``"graphql_event"`` —
        # any other value will fail to dispatch on the consumer.  In most
        # cases prefer ``broadcast_instance`` / ``broadcast_instance_grouped``
        # which handle serialization and fan-out for you.
        async_to_sync(channel_layer.send)(sock_key, {
            "type": "graphql_event",
            "graphql_field": "categoriesByBudget",
            "op_id": op_id,
            "payload": payload,
        })
    """
    _ensure_discovery()
    serializer = SerializerFactory.get(graphql_type, serializer_class)
    return serializer.serialize(model_instance, context=context or {})
