# Onliner Transformations for Apache Kafka® Connect

A collection of [Single Message Transformations (SMTs)](https://kafka.apache.org/documentation/#connect_transforms) for Apache Kafka Connect.

## Transformations

See [the Kafka documentation](https://kafka.apache.org/documentation/#connect_transforms) for more details about configuring transformations.

### `JsonSerialize`

This transformation serialize part of the original record's data to JSON strings.

The transformation:
- expects the record value/key to be either a `STRUCT` or a `MAP`;
- expects it to have a specified field;

Exists in two variants:
- `org.onliner.kafka.transforms.JsonSerialize$Key` - works on keys;
- `org.onliner.kafka.transforms.JsonSerialize$Value` - works on values.

The transformation defines the following configurations:
- `fields` - List of fields to serialize. Cannot be `null` or empty.

- Here's an example of this transformation configuration:

```properties
transforms=encode
transforms.encode.type=org.onliner.kafka.transforms.JsonSerialize$Value
transforms.encode.fields=comma,separated,list,of,fields
```

## License

This project is licensed under the [MIT license](LICENSE).

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
