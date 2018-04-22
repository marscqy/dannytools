package logging

import (
	"strings"
	"dannytools/constvar"
	"fmt"
	"os"

	myLogger "github.com/sirupsen/logrus"
	kitsFile "github.com/toolkits/file"
	kitsSlice "github.com/toolkits/slice"
)

const (
	DEBUG   = "debug"
	INFO    = "info"
	WARNING = "warning"
	ERROR   = "error"
	//CRITICAL = "critical"
)

var (
	LogLevelList []string = []string{
		DEBUG,
		INFO,
		WARNING,
		ERROR,
		//CRITICAL,
	}
	LogLevelToLogrusLevel map[string]myLogger.Level = map[string]myLogger.Level{
		DEBUG:   myLogger.DebugLevel,
		INFO:    myLogger.InfoLevel,
		WARNING: myLogger.WarnLevel,
		ERROR:   myLogger.ErrorLevel,
	}
)

type LogConf struct {
	LogFile   string `mapstructure:"logfile"`
	LogLevel  string `mapstructure:"loglevel"`
	LogFormat string `mapstructure:"logformat"`
}

func CheckLogLevel(lv string) bool {
	return kitsSlice.ContainsString(LogLevelList, lv)
}

func GetLogrusLogLevel(lv string) myLogger.Level {
	if !CheckLogLevel(lv) {
		lv = INFO
	}
	return LogLevelToLogrusLevel[lv]
}

func GetAllLogLevelsString(sep string) string {
	return strings.Join(LogLevelList, sep)
}

func SetLogLevel(lg *myLogger.Logger, lv string) {

	lg.Level = GetLogrusLogLevel(lv)
}

/*
use before we get logging config
*/

func NewRawLogger(lv string) *myLogger.Logger {
	lg := myLogger.New()
	lg.Level = GetLogrusLogLevel(lv)
	lg.Formatter = GetTextFormat()
	//lg.Formatter = myLogger.TextFormatter{ForceColors: false, DisableColors: true, DisableTimestamp: false, TimestampFormat: constvar.DATETIME_FORMAT_NOSPACE}
	return lg
}

func GetTextFormat() *myLogger.TextFormatter {
	return &myLogger.TextFormatter{ForceColors: false, DisableColors: true, DisableTimestamp: false, TimestampFormat: constvar.DATETIME_FORMAT_NOSPACE}
}

func GetJsonFormat() *myLogger.JSONFormatter {
	return &myLogger.JSONFormatter{TimestampFormat: constvar.DATETIME_FORMAT_NOSPACE, DisableTimestamp: false}
}

/*
logFile: full path of log file. default, os.Stdout is used
logLevel: debug, info, warning, error. default warning is used
format: json, text
*/
func (logCf *LogConf) CreateNewLogger() *myLogger.Logger {

	oneLogger := &myLogger.Logger{}

	if logCf.LogFile == "" {
		oneLogger.Out = os.Stdout
	} else {
		oneLogger.Out = kitsFile.MustOpenLogFile(logCf.LogFile)
	}

	if logCf.LogFormat == "json" {
		oneLogger.Formatter = GetJsonFormat()
	} else {
		oneLogger.Formatter = GetTextFormat()
	}

	if !CheckLogLevel(logCf.LogLevel) {
		fmt.Printf("unsupported loglevel '%s', set it to %s", logCf.LogLevel, WARNING)
		logCf.LogLevel = WARNING
	}

	oneLogger.Level = LogLevelToLogrusLevel[logCf.LogLevel]

	return oneLogger

}

func WriteToLogNoExtraMsg(logWr *myLogger.Logger, fields myLogger.Fields, level string) {
	WriteToLog(logWr, fields, "", level)
}

func WriteToLog(logWr *myLogger.Logger, fields myLogger.Fields, msg string, level string) {
	//fields["errcode"] = errCode
	switch level {
	case ERROR:
		logWr.WithFields(fields).Errorln(msg)
	case WARNING:
		logWr.WithFields(fields).Warningln(msg)
	case INFO:
		logWr.WithFields(fields).Infoln(msg)
	case DEBUG:
		logWr.WithFields(fields).Debugln(msg)
	default:
		logWr.WithFields(fields).Infoln(msg)
	}
}

func WriteLogOnlyMsg(logWr *myLogger.Logger, msg string, level string) {
	switch level {
	case ERROR:
		logWr.WithFields(myLogger.Fields{}).Errorln(msg)

	case WARNING:
		logWr.Warningln(msg)
	case INFO:
		logWr.Infoln(msg)
	case DEBUG:
		logWr.Debugln(msg)
	default:
		logWr.Infoln(msg)
	}
}
