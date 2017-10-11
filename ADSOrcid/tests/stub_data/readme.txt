generated as:

0000-0003-2686-9241
curl -H "Accept: application/json" 'http://pub.orcid.org/v1.2/0000-0003-2686-9241/orcid-bio' | python -m json.tool
curl -H "http://localhost:8984/solr/collection1/select?q=orcid%3A0000000326869241%0A&fl=bibcode%2Cauthor%2Cauthor_norm%2Corcid%2Cauthor_norm&wt=json&indent=true&facet=true&facet.prefix=1%2FStern%2C+D&facet=true&facet.field=author_facet_hier&facet.limit=20&facet.mincount=1&facet.offset=0"