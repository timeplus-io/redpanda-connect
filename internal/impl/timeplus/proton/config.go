package proton

type DriverConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	MaxIdleConns int
}
