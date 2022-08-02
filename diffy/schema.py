"""
.. module: diffy.plugins.schema
    :platform: Unix
    :copyright: (c) 2018 by Netflix Inc., see AUTHORS for more
    :license: Apache, see LICENSE for more details.
.. moduleauthor:: Kevin Glisson <kglisson@netflix.com>
"""

from inflection import underscore, camelize
from marshmallow import fields, Schema, post_load, pre_load, post_dump
from marshmallow.exceptions import ValidationError

from diffy.config import CONFIG
from diffy.plugins.base import plugins


class DiffySchema(Schema):
    """
    Base schema from which all diffy schema's inherit
    """

    __envelope__ = True

    def under(self, data, many=None):
        if many:
            return [{underscore(key): value for key, value in i.items()} for i in data]
        return {underscore(key): value for key, value in data.items()}

    def camel(self, data, many=None):
        if many:
            return [
                {
                    camelize(key, uppercase_first_letter=False): value
                    for key, value in i.items()
                }
                for i in data
            ]

        return {
            camelize(key, uppercase_first_letter=False): value
            for key, value in data.items()
        }

    def wrap_with_envelope(self, data, many):
        if many and "total" in self.context.keys():
            return dict(total=self.context["total"], items=data)
        return data


class DiffyInputSchema(DiffySchema):
    @pre_load(pass_many=True)
    def preprocess(self, data, many):
        return self.under(data, many=many)


class DiffyOutputSchema(DiffySchema):
    @pre_load(pass_many=True)
    def preprocess(self, data, many):
        if many:
            data = self.unwrap_envelope(data, many)
        return self.under(data, many=many)

    def unwrap_envelope(self, data, many):
        if many:
            if data["items"]:
                self.context["total"] = data["total"]
            else:
                self.context["total"] = 0
                data = {"items": []}

            return data["items"]

        return data

    @post_dump(pass_many=True)
    def post_process(self, data, many):
        if data:
            data = self.camel(data, many=many)
        return self.wrap_with_envelope(data, many=many) if self.__envelope__ else data


def resolve_plugin_slug(slug):
    """Attempts to resolve plugin to slug."""
    if plugin := plugins.get(slug):
        return plugin
    else:
        raise ValidationError(f"Could not find plugin. Slug: {slug}")


class PluginOptionSchema(Schema):
    options = fields.Dict(missing={})


class PluginSchema(DiffyInputSchema):
    options = fields.Dict(missing={})

    @post_load
    def post_load(self, data):
        data["plugin"] = resolve_plugin_slug(data["slug"])
        data["options"] = data["plugin"].validate_options(data["options"])
        return data


class TargetPluginSchema(PluginSchema):
    slug = fields.String(
        missing=CONFIG["DIFFY_TARGET_PLUGIN"],
        default=CONFIG["DIFFY_TARGET_PLUGIN"],
        required=True,
    )


class PersistencePluginSchema(PluginSchema):
    slug = fields.String(
        missing=CONFIG["DIFFY_PERSISTENCE_PLUGIN"],
        default=CONFIG["DIFFY_PERSISTENCE_PLUGIN"],
        required=True,
    )


class CollectionPluginSchema(PluginSchema):
    slug = fields.String(
        missing=CONFIG["DIFFY_COLLECTION_PLUGIN"],
        default=CONFIG["DIFFY_COLLECTION_PLUGIN"],
        required=True,
    )


class PayloadPluginSchema(PluginSchema):
    slug = fields.String(
        missing=CONFIG["DIFFY_PAYLOAD_PLUGIN"],
        default=CONFIG["DIFFY_PAYLOAD_PLUGIN"],
        required=True,
    )


class AnalysisPluginSchema(PluginSchema):
    slug = fields.String(
        missing=CONFIG["DIFFY_ANALYSIS_PLUGIN"],
        default=CONFIG["DIFFY_ANALYSIS_PLUGIN"],
        required=True,
    )
