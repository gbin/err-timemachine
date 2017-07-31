Interpreted your query as {{query}}

{% for result in results %}
{{result.ts.strftime('%Y-%m-%d %H:%M:%S')}} {{result.from_node}} : {{result.body}}
{% endfor %}
