package weasels

const (
	ErrCodeBadger = "badger_failure"
	ErrCodeEmpty  = "empty_database"
	ErrCodeArg    = "invalid_argument"
	ErrCodeCommit = "commit_failure"
)

type Error struct {
	Code    string
	Message string
	Cause   error
}

func newError(code, message string, cause error) *Error {
	return &Error{code, message, cause}
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}

	msg := ""
	switch {
	case e.Code != "" && e.Message != "":
		msg = "(" + e.Code + ") " + e.Message
	case e.Code != "":
		msg = e.Code
	case e.Message != "":
		msg = e.Message
	default:
		msg = "unknown error"
	}

	if e.Cause != nil {
		msg += " caused by " + e.Cause.Error()
	}
	return msg
}
