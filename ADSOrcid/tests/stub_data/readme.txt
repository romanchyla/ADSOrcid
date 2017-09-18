generated as:

0000-0003-2686-9241
Stored in the “profile” attribute in 0000-0003-3041-2092.ads.json:
curl -H "Accept: application/json" 'https://pub.orcid.org/v2.0/0000-0003-3041-2092/record' | python -m json.tool

Stored in 0000-0003-2686-9241.orcid.json:
curl -H "Accept: application/json" 'https://pub.orcid.org/v2.0/0000-0003-2686-9241/record' | python -m json.tool

curl -H "http://localhost:8984/solr/collection1/select?q=orcid%3A0000000326869241%0A&fl=bibcode%2Cauthor%2Cauthor_norm%2Corcid%2Cauthor_norm&wt=json&indent=true&facet=true&facet.prefix=1%2FStern%2C+D&facet=true&facet.field=author_facet_hier&facet.limit=20&facet.mincount=1&facet.offset=0"