# football_dataset
This project contains all that is needed to have a proper dataset for the Football36 app.

</br>

<b>REQUIREMENTS<b/>
<ul>
  <li>Python 3.11</li>
  <li>PostgreSQL>=15</li>
  <li>Neo4j>=5.6.0</li>
  <li>a subscription for the API-Football api</li>
</ul>  

</br>STEPS</br> (this might take hours, even days)
<ol>
  <li>Run <code>data_generator/collect_data.py</code></li>
  <li>Run the function <code>download_images</code> from <code>data_generator/data_fixers.py</code></li>
  <li>Run the function <code>fix_transfers</code> from <code>data_generator/data_fixers.py</code></li>
  <li>Run the function <code>collect_valuable_players</code> from <code>api_client/transfermarkt_scraper.py</code></li>
  <li>Run the function <code>collect_valuable_teams</code> from <code>api_client/transfermarkt_scraper.py</code></li>
  <li>Run <code>data_generator/entity_values_maker.py</code></li>
  <li>Run <code>data_generator/neo4j_interactor.py</code></li>
<ol>
