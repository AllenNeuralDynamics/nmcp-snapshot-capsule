from typing import List

from gql import Client
from gql.transport.requests import RequestsHTTPTransport

from queries import published_reconstructions_query

DEFAULT_HOST = "https://morphology.allenneuraldynamics.org/graphql"


def query_published(limit: int = 0, offset: int = 0, host: str = DEFAULT_HOST):
    transport = RequestsHTTPTransport(
        url=host,
        verify=True,
        retries=3,
        headers={"Content-Type": "application/json"}
    )

    client = Client(transport=transport, fetch_schema_from_transport=False)

    params = {}
    if limit > 0:
        params["limit"] = limit
    if offset > 0:
        params["offset"] = offset

    result = client.execute(published_reconstructions_query, variable_values=params)
    return result["publishedReconstructions"]["reconstructions"]
