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

### `JsonDeserialize`

This transformation deserialize JSON strings of the original record's data to structure.

The transformation:
- expects the record value/key to be a `JSON` string;
- expects it to have a specified field;
- expects `JSON` string doesn't contain arrays;

Exists in two variants:
- `org.onliner.kafka.transforms.JsonDeserialize$Key` - works on keys;
- `org.onliner.kafka.transforms.JsonDeserialize$Value` - works on values.

The transformation defines the following configurations:
- `fields` - List of fields to serialize. Cannot be `null` or empty.

- Here's an example of this transformation configuration:

```properties
transforms=decode
transforms.decode.type=org.onliner.kafka.transforms.JsonDeserialize$Value
transforms.decode.fields=comma,separated,list,of,fields
```

### `ConcatFields`

This transformation concat fields of the original record's data to single string with delimiter.

The transformation:
- expects the record value/key to be either a `STRUCT` or a `MAP`;

Exists in two variants:
- `org.onliner.kafka.transforms.ConcatFields$Key` - works on keys;
- `org.onliner.kafka.transforms.ConcatFields$Value` - works on values.

The transformation defines the following configurations:
- `fields` - List of fields to concat. Cannot be `null` or empty.
- `delimiter` - Delimiter for concat. Cannot be `null` or empty.
- `output` - Output field. Cannot be `null` or empty.

```properties
transforms=concat
transforms.concat.type=org.onliner.kafka.transforms.ConcatFields$Value
transforms.concat.fields=latitude,longitude
transforms.concat.delimiter=,
transforms.concat.output=location
```

## License

This project is licensed under the [MIT license](LICENSE).

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
