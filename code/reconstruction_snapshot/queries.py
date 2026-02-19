from gql import gql

published_reconstructions_query = gql(
    """
    query PublishedReconstructions($offset: Int, $limit: Int) {
      publishedReconstructions(offset: $offset, limit: $limit) {
        total
        offset
        reconstructions {
          id
          sourceUrl
          neuron {
            id
            label
            specimen {
              id
              label
            }
          }
        }
      }
    }
    """
)
