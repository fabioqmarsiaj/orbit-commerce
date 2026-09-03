package schema

import _ "embed"

//go:embed avro/ProductViewed.avsc
var productViewedSchemaJSON string

//go:embed avro/AddedToCart.avsc
var addedToCartSchemaJSON string

//go:embed avro/SearchPerformed.avsc
var searchPerformedSchemaJSON string
