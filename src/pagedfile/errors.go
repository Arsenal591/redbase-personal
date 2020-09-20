package pagedfile

import "errors"

var (
	ErrPageBeingUsed       = errors.New("The page is being used.")
	ErrNoAvailablePage     = errors.New("There is no avaiable page now.")
	ErrPageAlreadyInBuffer = errors.New("The page is already in buffer pool.")
	ErrPageNotInBuffer     = errors.New("The page is not in buffer pool.")
	ErrPageNotInUse        = errors.New("The page is not in use.")
)
