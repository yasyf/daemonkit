package daemonkit

import "testing"

func TestRequirementDigest(t *testing.T) {
	tests := []struct {
		name string
		req  Requirement
		want PolicyDigest
	}{
		{
			"holder",
			Requirement{TeamID: "SXKCTF23Q2", SigningIdentifier: "com.yasyf.daemonkit.holder"},
			"d15a4214943a8f565c041a9603ef0ac72c1d99f62902841bbd5ad86fbadb4a98",
		},
		{
			"zero",
			Requirement{},
			"374708fff7719dd5979ec875d56cd2286f6d3cf7ec317a3b25632aab28ec37bb",
		},
		{
			"field boundary AB/C",
			Requirement{TeamID: "AB", SigningIdentifier: "C"},
			"45f797d3f083d395969237f6d394b60f5d1ddf20aa2b3f6608a3f3439844b00e",
		},
		{
			"field boundary A/BC",
			Requirement{TeamID: "A", SigningIdentifier: "BC"},
			"87b80d62c2ccdbcfffbfc12d3d1ef2dbd4fb61235438df0bbac226cf5bd21ce9",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.req.Digest(); got != tt.want {
				t.Errorf("Digest() = %q, want %q", got, tt.want)
			}
		})
	}
}
