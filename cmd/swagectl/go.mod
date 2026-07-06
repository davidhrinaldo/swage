module github.com/davidrinaldo/swage/cmd/swagectl

go 1.26.1

require (
	github.com/davidrinaldo/swage v0.0.0
	github.com/davidrinaldo/swage/ingotstore v0.0.0
)

require github.com/davidhrinaldo/ingot v0.1.1 // indirect

replace (
	github.com/davidrinaldo/swage => ../..
	github.com/davidrinaldo/swage/ingotstore => ../../ingotstore
)
