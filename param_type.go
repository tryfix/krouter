package krouter

type ParamType string

const (
	ParamTypeInt    ParamType = `Int`
	ParamTypeString ParamType = `String`
	ParamTypeUuid   ParamType = `Uuid`
)

type Param struct {
	name      string
	typ       ParamType
	whenEmpty func() string
}

type CustomParam struct {
	typ     ParamType
	decoder func(v string) (interface{}, error)
}
