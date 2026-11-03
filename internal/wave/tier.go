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
	"fmt"
	"strings"
)

// Canonical tier names and aliases.
const (
	TierDev     = "dev"
	TierTest    = "test"
	TierStaging = "staging"
	TierProd    = "prod"

	aliasDevelopment = "development"
	aliasQA          = "qa"
	aliasStage       = "stage"
	aliasPreprod     = "preprod"
	aliasProduction  = "production"
)

// Environments are ordered by fixed tiers, earliest first. Every tier accepts
// a few aliases; the first name is canonical. Aliases are the same environment
// everywhere: wave grouping, upstream lookup and GitHub matching.
var tiers = [][]string{
	{TierDev, aliasDevelopment},
	{TierTest, aliasQA},
	{TierStaging, aliasStage, aliasPreprod},
	{TierProd, aliasProduction},
}

// Canonical returns the canonical tier name of an environment name and whether
// the name is a known tier. Unknown names are returned unchanged: they belong
// to no tier and never act as an upstream.
func Canonical(name string) (string, bool) {
	for _, t := range tiers {
		for _, a := range t {
			if a == name {
				return t[0], true
			}
		}
	}
	return name, false
}

// Rank is the position of the tier of name (0 = earliest) or -1 when unknown.
func Rank(name string) int {
	for i, t := range tiers {
		for _, a := range t {
			if a == name {
				return i
			}
		}
	}
	return -1
}

// Aliases returns every accepted name of the tier of name, canonical first.
// An unknown name returns itself.
func Aliases(name string) []string {
	if r := Rank(name); r >= 0 {
		return tiers[r]
	}
	return []string{name}
}

// EarlierTiers returns the canonical names of the tiers before name's tier,
// nearest first. Unknown names have none.
func EarlierTiers(name string) []string {
	r := Rank(name)
	var out []string
	for i := r - 1; i >= 0; i-- {
		out = append(out, tiers[i][0])
	}
	return out
}

// UnknownTierMessage explains that name is not a tier and lists the accepted names.
func UnknownTierMessage(name string) string {
	var canon, aliases []string
	for _, t := range tiers {
		canon = append(canon, t[0])
		aliases = append(aliases, t[1:]...)
	}
	return fmt.Sprintf("environment %q is not a known tier; use one of %s (aliases: %s)",
		name, strings.Join(canon, ", "), strings.Join(aliases, ", "))
}
