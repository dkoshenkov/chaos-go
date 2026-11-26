package server

import "runtime/debug"

var (
	version   = "dev"
	commit    string
	buildDate string
)

type versionInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
}

func currentVersion() versionInfo {
	info := versionInfo{Version: version, Commit: commit, BuildDate: buildDate}
	if build, ok := debug.ReadBuildInfo(); ok {
		if info.Version == "dev" && build.Main.Version != "" && build.Main.Version != "(devel)" {
			info.Version = build.Main.Version
		}
		for _, setting := range build.Settings {
			if setting.Key == "vcs.revision" && info.Commit == "" {
				info.Commit = setting.Value
			}
		}
	}
	return info
}
