"""
Identifiers are used by products to represent their persistent
representations, for example product, for example, File uses it to represent
the path to a file. Identifiers are lazy-loaded, they can be initialized
with a jinja2.Template and rendered before task execution, which makes
passing metadata between products and its upstream tasks possible.
"""
