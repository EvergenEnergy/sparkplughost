package sparkplughost

import "errors"

var errOutOfSync = errors.New("host application is out of sync. A Node rebirth is needed")
