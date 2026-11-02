/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package wave

import (
	"reflect"
	"testing"
)

func TestTiers(t *testing.T) {
	for in, want := range map[string]string{"production": "prod", "prod": "prod", "qa": "test", "preprod": "staging", "stage": "staging", "development": "dev", "x": "x"} {
		if got, _ := Canonical(in); got != want {
			t.Errorf("Canonical(%q) = %q, want %q", in, got, want)
		}
	}
	if got := EarlierTiers("production"); !reflect.DeepEqual(got, []string{"staging", "test", "dev"}) {
		t.Errorf("EarlierTiers(production) = %v", got)
	}
	if got := EarlierTiers("dev"); got != nil {
		t.Errorf("EarlierTiers(dev) = %v", got)
	}
	if got := EarlierTiers("custom"); got != nil {
		t.Errorf("EarlierTiers(custom) = %v", got)
	}
	if got := Aliases("stage"); !reflect.DeepEqual(got, []string{"staging", "stage", "preprod"}) {
		t.Errorf("Aliases(stage) = %v", got)
	}
	if Rank("qa") != 1 || Rank("nope") != -1 {
		t.Errorf("rank")
	}
}
