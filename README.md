# Centarro Search

The Centarro Search module offers an implementation of the Search API that uses an Elastic Enterprise server for 
indexing content (version 8.x).

It uses the Elastic Search API for indexing/updating/deleting documents and the Elastic APP Search API for Search. 

For a full description of the module, visit the
[project page](https://www.drupal.org/project/centarro_search).

## Table of contents

- Requirements
- Installation
- Configuration

## Requirements

This module requires the following modules:

- [Search API](https://www.drupal.org/project/search_api)

This module requires the following libraries:

- [Elasticsearch PHP client](https://github.com/elastic/elasticsearch-php)
- [elastic/enterprise-search](https://github.com/elastic/enterprise-search-php)

## Installation

Install as you would normally install a contributed Drupal module. For further
information, see
[Installing Drupal Modules](https://www.drupal.org/docs/extending-drupal/installing-drupal-modules).

## Configuration

First, create an "Elastic Enterprise Search (Centarro Search)" Search API server, and an index using it.
On the Search API index configuration page, configure the engine and the index to use.
