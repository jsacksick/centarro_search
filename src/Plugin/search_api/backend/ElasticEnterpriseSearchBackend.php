<?php

namespace Drupal\centarro_search\Plugin\search_api\backend;

use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Plugin\PluginFormInterface;
use Drupal\search_api\IndexInterface;
use Drupal\search_api\Plugin\PluginFormTrait;
use Drupal\search_api\Plugin\search_api\data_type\value\TextValueInterface;
use Drupal\search_api\Query\ConditionGroupInterface;
use Drupal\search_api\Query\QueryInterface;
use Drupal\search_api\Backend\BackendPluginBase;
use Drupal\search_api\Query\ResultSet;
use Drupal\search_api\SearchApiException;
use Elastic\Elasticsearch\ClientBuilder;
use Elastic\EnterpriseSearch\AppSearch\Request\CreateEngine;
use Elastic\EnterpriseSearch\AppSearch\Request\GetEngine;
use Elastic\EnterpriseSearch\AppSearch\Request\Search;
use Elastic\EnterpriseSearch\AppSearch\Schema\Engine;
use Elastic\EnterpriseSearch\AppSearch\Schema\PaginationResponseObject;
use Elastic\EnterpriseSearch\AppSearch\Schema\SearchRequestParams;
use Elastic\EnterpriseSearch\AppSearch\Schema\SimpleObject;
use Elastic\EnterpriseSearch\Client;
use Elastic\Transport\TransportBuilder;

/**
 * Elastic Enterprise Search backend for search api.
 *
 * @SearchApiBackend(
 *   id = "centarro_search_ees",
 *   label = @Translation("Elastic Enterprise Search (Centarro Search)"),
 *   description = @Translation("Index items using an Elastic Enterprise search server.")
 * )
 */
class ElasticEnterpriseSearchBackend extends BackendPluginBase implements PluginFormInterface {

  use PluginFormTrait {
    submitConfigurationForm as traitSubmitConfigurationForm;
  }

  /**
   * The Elastic client.
   *
   * @var \Elastic\Elasticsearch\Client
   */
  protected $client;

  /**
   * The Elastic Enterprise client.
   *
   * @var \Elastic\EnterpriseSearch\Client
   */
  protected $enterpriseClient;

  /**
   * {@inheritdoc}
   */
  public function defaultConfiguration() {
    return [
      'elastic_api_key' => '',
      'elastic_cloud_id' => '',
      'app_search_host' => '',
      'app_search_api_key' => '',
      'ca_crt' => '',
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function indexItems(IndexInterface $index, array $items) {
    $client = $this->getClient();
    $params = [];
    /** @var \Drupal\search_api\Item\ItemInterface[] $items */
    foreach ($items as $id => $item) {
      $document = [
        'language' => $item->getLanguage(),
        'search_api_datasource' => $item->getDatasourceId(),
      ];
      $fields = $item->getFields();
      foreach ($fields as $field) {
        $values = $field->getValues();
        // @todo check if we should we skip empty values here?
        if (empty($values)) {
          continue;
        }
        $values = array_map(function ($value) {
          return $value instanceof TextValueInterface ? $value->toText() : $value;
        }, $values);
        // @todo figure out what to do exactly here (do some processing?).
        $document += [
          $field->getFieldIdentifier() => count($values) === 1 ? reset($values) : $values,
        ];
      }
      $params['body'][] = [
        'index' => [
          '_index' => $this->getElasticIndexName($index),
          '_id' => $id,
        ]
      ];
      $params['body'][] = $document;
    }
    $response = $client->bulk($params)->asArray();
    if (empty($response['items'])) {
      return [];
    }

    return array_map(function ($item) {
      return $item['index']['_id'];
    }, $response['items']);
  }

  /**
   * {@inheritdoc}
   */
  public function deleteItems(IndexInterface $index, array $item_ids) {
    // @todo figure out how to delete multiple items at once.
    try {
      $client = $this->getClient();
      /*$client->deleteByQuery([
        'index' => $this->getElasticIndexName($index),
        'body' => [
          'query' => [
            'match_all' => new \stdClass(),
          ],
        ],
      ]);*/
    }
    catch (\Exception $exception) {
      throw new SearchApiException($exception->getMessage());
    }
  }

  /**
   * {@inheritdoc}
   */
  public function deleteAllIndexItems(IndexInterface $index, $datasource_id = NULL) {
    $client = $this->getClient();
    try {
      // @todo use the $datasource_id?
      $client->deleteByQuery([
        'index' => $this->getElasticIndexName($index),
        'body' => [
          'query' => [
            'match_all' => new \stdClass(),
          ],
        ],
      ]);
    }
    catch (\Exception $exception) {
      throw new SearchApiException($exception->getMessage());
    }
  }

  /**
   * {@inheritdoc}
   */
  public function search(QueryInterface $query) {
    $original_query = clone $query;
    $response = $this->doSearch($query);
    $index = $query->getIndex();
    $results = $query->getResults();

    if (empty($response['results'])) {
      return;
    }

    $results->setResultCount($response['meta']['page']['total_results']);
    foreach ($response['results'] as $result) {
      if (!isset($result['_meta']['id'])) {
        continue;
      }
      $result_item = $this->fieldsHelper->createItem($index, $result['_meta']['id']);
      $result_item->setScore($result['_meta']['score']);
      foreach ($result as $name => $field_value) {
        if ($name === '_meta' || !isset($field_value['raw'])) {
          continue;
        }
        $field = $this->fieldsHelper->createField($index, $name);
        $values = !is_array($field_value['raw']) ? [$field_value['raw']] : $field_value['raw'];
        $field->setValues($values);
        $result_item->setField($name, $field);
      }
      $results->addResultItem($result_item);
    }

    // Handle the facets result when enabled.
    if ($facets = $query->getOption('search_api_facets')) {
      $or_facets = array_filter($facets, function ($facet) {
        return $facet['operator'] === 'or';
      });
      // When there are "OR" search facets, we need to perform an additional
      // query for each of the "OR" facet in which we remove the current
      // facet from the query filters.
      if ($or_facets) {
        foreach ($or_facets as $name => $facet) {
          $facet_query = clone $original_query;
          // Fake the "search_api_facets" option so that only the current facet
          // is handled in parseFacets().
          $facet_query->setOption('search_api_facets', array_intersect_key($facets, [$name => $name]));
          // For each of the "OR" facet, we need to perform a query that has
          // all the filters, besides the current facet.
          $facet_query->setOption('filters_to_skip', [$name]);
          $facet_query_response = $this->doSearch($facet_query);
          $this->parseFacets($results, $facet_query, $facet_query_response);
        }
      }
      // Extract the remaining facets that aren't "OR" facets, note that we
      // can use the original query for that.
      if (count($or_facets) !== count($facets)) {
        $original_query->setOption('search_api_facets', array_diff_key($facets, $or_facets));
        $this->parseFacets($results, $original_query, $response);
      }
    }
  }

  /**
   * Executes a search on this server.
   *
   * @param \Drupal\search_api\Query\QueryInterface $query
   *   The query to execute.
   *
   * @return array
   *   The search response.
   *
   * @throws \Drupal\search_api\SearchApiException
   *   Thrown if an error prevented the search from completing.
   */
  protected function doSearch(QueryInterface $query) {
    $filters = $this->buildFilters($query->getConditionGroup(), $query);
    $search_query = $query->getKeys() ?? '';
    $search = new SearchRequestParams($search_query);
    // @todo check if filters & facets are correctly passed.
    if ($filters) {
      $filters_object = new SimpleObject();
      foreach ($filters as $key => $filter) {
        $filters_object->$key = $filter;
      }
      $search->filters = $filters_object;
    }
    $facets = $this->buildFacets($query);
    if ($facets) {
      $facets_object = new SimpleObject();
      foreach ($facets as $key => $facet) {
        $facets_object->$key = $facet;
      }
      $search->facets = $facets_object;
    }
    $limit = $query->getOption('limit');
    $offset = $query->getOption('offset');

    $pagination = new PaginationResponseObject();
    // Handle pagination.
    if ($limit) {
      $pagination->size = $limit;
    }
    if ($offset) {
      // The "current" page starts at 1 for the first page, therefore we need
      // to add 1.
      $pagination->current = (int) (ceil($offset / $limit) + 1);
    }
    $search->page = $pagination;
    // @todo this needs to handle fields that can't be sorted on.
    foreach ($query->getSorts() as $field => $order) {
      // Relevance is a special field.
      $field = $field === 'search_api_relevance' ? '_score' : $field;
      $search->sort[] = [$field => strtolower($order)];
    }
    $index = $query->getIndex();
    try {
      $client = $this->getEnterpriseClient();
      // Perform the search query.
      $request = $client->appSearch()->search(
        new Search(
          $this->getEngineName($index),
          $search,
        )
      );

      return $request->asArray();
    }
    catch (\Exception $exception) {
      $this->getLogger()->error($exception->getMessage());
      throw new SearchApiException($exception->getMessage());
    }
  }

  /**
   * {@inheritdoc}
   */
  public function buildConfigurationForm(array $form, FormStateInterface $form_state) {
    $form['elastic_api_key'] = [
      '#type' => 'textfield',
      '#title' => $this->t('Elastic API key'),
      '#default_value' => $this->configuration['elastic_api_key'],
    ];
    $form['elastic_cloud_id'] = [
      '#type' => 'textfield',
      '#title' => $this->t('Elastic Search Cloud ID'),
      '#default_value' => $this->configuration['elastic_cloud_id'],
      '#maxlength' => 300,
    ];
    $form['app_search_host'] = [
      '#type' => 'textfield',
      '#title' => $this->t('APP Search Host'),
      '#default_value' => $this->configuration['app_search_host'],
      '#required' => TRUE,
    ];
    $form['app_search_api_key'] = [
      '#type' => 'textfield',
      '#title' => $this->t('APP Search API Key'),
      '#default_value' => $this->configuration['app_search_api_key'],
      '#required' => TRUE,
    ];
    $form['ca_crt'] = [
      '#type' => 'textfield',
      '#title' => $this->t('CA certificate'),
      '#description' => $this->t('Path to the CA certificate'),
      '#default_value' => $this->configuration['ca_crt'],
    ];

    return $form;
  }

  /**
   * {@inheritdoc}
   */
  public function validateConfigurationForm(array &$form, FormStateInterface $form_state) {}

  /**
   * {@inheritdoc}
   */
  public function submitConfigurationForm(array &$form, FormStateInterface $form_state) {
    $this->traitSubmitConfigurationForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function getSupportedFeatures() {
    return [
      'search_api_facets',
      'search_api_facets_operator_or',
    ];
  }

  /**
   * {@inheritdoc}
   */
  public function updateIndex(IndexInterface $index) {
    try {
      $this->ensureEngine($index);
    }
    catch (\Exception $exception) {
      $this->getLogger()->error($exception->getMessage());
    }
  }

  /**
   * Recursively build the search "filters".
   *
   * @param \Drupal\search_api\Query\ConditionGroupInterface $condition_group
   *   The group of conditions.
   * @param \Drupal\search_api\Query\QueryInterface $query
   *   The query.
   *
   * @return array
   *   The search filters.
   */
  protected function buildFilters(ConditionGroupInterface $condition_group, QueryInterface $query) {
    $conjuction_mapping = [
      'AND' => 'all',
      'OR' => 'any',
    ];
    if (!isset($conjuction_mapping[$condition_group->getConjunction()])) {
      return [];
    }
    $filters_to_skip = $query->getOption('filters_to_skip', []);
    $operator_mapping = [
      '<>' => 'none',
    ];
    $index = $query->getIndex();
    $field_types = [];
    // Builds an array holding the field type for each field.
    foreach ($index->getFields() as $name => $field) {
      $field_types[$name] = $field->getType();
    }
    $filter_group = $conjuction_mapping[$condition_group->getConjunction()];
    $filters = [];
    // Build the filters.
    foreach ($condition_group->getConditions() as $condition) {
      if ($condition instanceof ConditionGroupInterface) {
        $filters[$filter_group][] = $this->buildFilters($condition, $query);
        continue;
      }
      $field = $condition->getField();
      // Skip the "OR" facets, if specified in the query options.
      if (!empty($filters_to_skip) && in_array($field, $filters_to_skip, TRUE)) {
        continue;
      }
      $operator = $condition->getOperator();
      // Check if the filter should belong to a "subgroup".
      $filter_subgroup = $operator_mapping[$operator] ?? NULL;
      $value = $condition->getValue();
      // Always pass an array to filters, passing an array with a single value
      // is equivalent to not passing an array and that makes our code easier.
      if (is_array($value)) {
        $value = array_values($value);
      }
      elseif (is_scalar($value)) {
        $value = [$value];
      }
      // We need to cast "number" fields, Elastic APP search rejects numbers
      // passed as strings.
      if (isset($field_types[$field]) && $field_types[$field] === 'integer') {
        $value = array_map('intval', $value);
      }
      elseif (isset($field_types[$field]) && $field_types[$field] === 'decimal') {
        $value = array_map('floatval', $value);
      }
      if ($operator === 'BETWEEN') {
        $value = [
          'from' => $value[0],
          'to' => $value[1],
        ];
      }
      // "<>" operators should have their own filter group.
      if ($filter_subgroup) {
        $filters[$filter_group][$filter_subgroup][][$field] = $value;
      }
      else {
        $filters[$filter_group][][$field] = $value;
      }
    }

    return $filters;
  }

  /**
   * Helper method for creating the "facets" query parameters.
   *
   * @param \Drupal\search_api\Query\QueryInterface $query
   *   The Search API query.
   *
   * @return array
   *   The "facets" query parameters.
   */
  protected function buildFacets(QueryInterface $query): array {
    $facets = $query->getOption('search_api_facets', []);
    if (empty($facets)) {
      return [];
    }
    // @todo Support more facets (ranges?).
    $facet_type_mapping = [
      'search_api_string' => 'value',
    ];
    $facet_params = [];
    foreach ($facets as $facet) {
      if (!isset($facet_type_mapping[$facet['query_type']])) {
        continue;
      }
      $facet_params[$facet['field']] = [
        'type' => $facet_type_mapping[$facet['query_type']],
        // 250 is the maximum limit, as specified in the Elastic APP search
        // documentation.
        'size' => 250,
      ];
    }

    return $facet_params;
  }

  /**
   * Gets an instantiated Elastic search client.
   *
   * @return \Elastic\Elasticsearch\Client
   *   The Elastic client.
   */
  protected function getClient() {
    if (isset($this->client)) {
      return $this->client;
    }
    $client = ClientBuilder::create()
      ->setHosts([$this->configuration['app_search_host']])
      ->setApiKey($this->configuration['elastic_api_key']);

    if (!empty($this->configuration['ca_crt'])) {
      $client->setCABundle($this->configuration['ca_crt']);
    }
    if (!empty($this->configuration['elastic_cloud_id'])) {
      $client->setElasticCloudId($this->configuration['elastic_cloud_id']);
    }
    // For whatever reason, the request crashes if no async HTTP client is set.
    // This is probably a bug in the SDK.
    $transport = TransportBuilder::create()->build();
    $client->setAsyncHttpClient($transport);
    $this->client = $client->build();
    return $this->client;
  }

  /**
   * Gets an instantiated Elastic Enterprise client.
   *
   * @return \Elastic\EnterpriseSearch\Client
   *   The Enterprise search client.
   */
  protected function getEnterpriseClient() {
    if (isset($this->enterpriseClient)) {
      return $this->enterpriseClient;
    }
    $configuration = [
      'host' => $this->configuration['app_search_host'],
      'app-search' => [
        'apiKey' => $this->configuration['app_search_api_key'],
      ],
    ];
    $client = new Client($configuration);
    $this->enterpriseClient = $client;
    return $this->enterpriseClient;
  }

  /**
   * Ensures an engine exists for the given index.
   *
   * @param \Drupal\search_api\IndexInterface $index
   *   The Search API index.
   *
   * @return string
   *   The engine name.
   */
  protected function ensureEngine(IndexInterface $index) {
    $client = $this->getEnterpriseClient();
    $engine_name = $this->getEngineName($index);
    try {
      $client->appSearch()->getEngine(
        new GetEngine($engine_name)
      );
    }
    catch (\Exception $exception) {
      $this->getLogger()->error($exception->getMessage());
      $engine = new Engine($engine_name);
      $engine->type = $this->getEngineType($index);
      $client->appSearch()->createEngine(new CreateEngine($engine));
    }

    return $engine_name;
  }

  /**
   * Gets the engine name for the given index.
   *
   * @param \Drupal\search_api\IndexInterface $index
   *   The Search API index.
   *
   * @return string
   *   The engine name.
   */
  protected function getEngineName(IndexInterface $index): string {
    $third_party_settings = $index->getThirdPartySettings('centarro_search');
    if (!empty($third_party_settings['engine'])) {
      return $third_party_settings['engine'];
    }

    // @todo remove this fallback?
    return str_replace('_', '-', $index->id());
  }

  /**
   * Gets the engine type for the given index.
   *
   * @param \Drupal\search_api\IndexInterface $index
   *   The Search API index.
   *
   * @return string
   *   The engine type.
   */
  protected function getEngineType(IndexInterface $index): string {
    $third_party_settings = $index->getThirdPartySettings('centarro_search');
    return $third_party_settings['engine_type'] ?? 'default';
  }

  /**
   * Gets the Elastic index to use for the given index.
   *
   * @param \Drupal\search_api\IndexInterface $index
   *   The Search API index.
   *
   * @return string
   *   The Elastic index name.
   */
  protected function getElasticIndexName(IndexInterface $index): string {
    $third_party_settings = $index->getThirdPartySettings('centarro_search');
    return $third_party_settings['index'] ?? $index->id();
  }

  /**
   * Parse the result set and add the facet values.
   *
   * @param \Drupal\search_api\Query\ResultSet $results
   *   Result set, all items matched in a search.
   * @param \Drupal\search_api\Query\QueryInterface $query
   *   Search API query object.
   * @param array $response
   *   The search response.
   */
  protected function parseFacets(ResultSet $results, QueryInterface $query, array $response) {
    if (!isset($response['facets'])) {
      return;
    }
    $facets = $query->getOption('search_api_facets');

    // Because this method is called from within a loop, reuse the FACETS
    // already stored in the results extra data so we don't override
    // already processed facets.
    $attach = $results->getExtraData('search_api_facets', []);
    foreach ($response['facets'] as $name => $data) {
      if (!isset($facets[$name])) {
        continue;
      }
      $terms = [];
      foreach ($data[0]['data'] as $value) {
        $terms[] = [
          'count' => $value['count'],
          'filter' => '"' . $value['value'] . '"',
        ];
      }
      $attach[$name] = $terms;
    }

    $results->setExtraData('search_api_facets', $attach);
  }

  /**
   * {@inheritdoc}
   */
  protected function validateOperator($operator) {
    switch ($operator) {
      case '=':
      case '<>':
      case 'BETWEEN':
        return;
    }
    throw new SearchApiException("Unknown operator '$operator' used in search query condition");
  }

  /**
   * Builds the index schema.
   *
   * @todo figure out how to specify the schema.
   *
   * @param \Drupal\search_api\IndexInterface $index
   *   The Search API index.
   *
   * @return array
   *   The schema.
   */
  protected function buildSchema(IndexInterface $index): array {
    $schema = [];
    $types_mapping = [
      'date' => 'date',
      'decimal' => 'number',
      'integer' => 'number',
      'string' => 'text',
      'text' => 'text',
    ];
    foreach ($index->getFields() as $field) {
      if (!isset($types_mapping[$field->getType()])) {
        continue;
      }
      $schema[$field->getFieldIdentifier()] = $types_mapping[$field->getType()];
    }

    return $schema;
  }

}
