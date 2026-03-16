from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from graphql import get_introspection_query, build_client_schema, print_schema

transport = RequestsHTTPTransport(
    url="https://morphology.allenneuraldynamics-test.org/graphql",
    headers={"content-type": "application/json"},
)

client = Client(transport=transport, fetch_schema_from_transport=False)

# Run a customized introspection query (e.g., include directive metadata)
q = get_introspection_query(
    descriptions=True,
    schema_description=True,
    specified_by_url=True,
    directive_is_repeatable=True,
    input_value_deprecation=True,
)
result = client.execute(gql(q))
schema = build_client_schema(result)
schema_str = print_schema(schema)
with open("schema.txt", 'w') as f:
    f.write(schema_str)
    