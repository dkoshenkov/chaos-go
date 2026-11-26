{{- define "chaos-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "chaos-proxy.labels" -}}
{{ include "chaos-proxy.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "chaos-proxy.portName" -}}
{{- if eq .Values.mode "http" -}}
http
{{- else if eq .Values.mode "tcp" -}}
tcp
{{- else if eq .Values.mode "connect" -}}
tcp-connect
{{- else -}}
{{- fail "mode must be http, tcp, or connect" -}}
{{- end -}}
{{- end -}}
