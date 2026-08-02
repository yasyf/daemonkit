//go:build mixedera

package coverage

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	dispositionClaimed = "claimed"
	dispositionAbsent  = "absent"
)

// EvidenceKind is one class of artifact a fact is read off.
type EvidenceKind string

// The three classes of artifact this gate redeems a claim against, each
// produced outside the case that redeems it.
const (
	FromProcessTable EvidenceKind = "process"
	FromWire         EvidenceKind = "wire"
	FromPeerVerdict  EvidenceKind = "peer"
)

var artifacts = map[EvidenceKind]string{
	FromProcessTable: "a process exit the OS reaped",
	FromWire:         "bytes the relay copied",
	FromPeerVerdict:  "the peer process's own verdict",
}

// Reservation is one witness mechanisms.txt reserves: the single site allowed
// to file one mechanism's evidence of one class.
type Reservation struct {
	Mechanism string
	Kind      EvidenceKind
	Site      string
}

// Reservations lists every witness mechanisms.txt reserves, mechanisms in the
// order that file declares them and classes sorted within each.
func Reservations() []Reservation {
	var held []Reservation
	for _, frozen := range denoted().ordered {
		for _, kind := range slices.Sorted(maps.Keys(frozen.witnesses)) {
			held = append(held, Reservation{Mechanism: frozen.name, Kind: kind, Site: frozen.witnesses[kind]})
		}
	}
	return held
}

// Reserves reports whether mechanisms.txt reserves this mechanism's evidence of
// this class for this site, and what it reserves instead when it does not.
// Keyed by mechanism alone the journal would take any real observation under
// any mechanism's name, which turns an unproven row green with the frozen file
// byte-identical: the name is all that file would bind.
func Reserves(site, mechanism string, kind EvidenceKind) error {
	held, frozen := denoted().named(mechanism)
	if !frozen {
		return fmt.Errorf("%s files evidence of %q, which %s names no mechanism", site, mechanism, MechanismPath)
	}
	registered, reserved := held.witnesses[kind]
	switch {
	case !reserved:
		return fmt.Errorf("%s files %q evidence of %q, and %s reserves no %s witness for that mechanism, which leaves it redeemable by entailment alone. Register this site there, in this same commit:\n  witness %s %s",
			site, artifacts[kind], mechanism, MechanismPath, kind, kind, site)
	case registered != site:
		return fmt.Errorf("%s files %q evidence of %q, which %s reserves for %s. One site files a mechanism's evidence, so an observation that site already made cannot be filed a second time under another mechanism's name",
			site, artifacts[kind], mechanism, MechanismPath, registered)
	}
	return nil
}

// mechanism is everything mechanisms.txt freezes under one name: the claim the
// name denotes, the one site allowed to file each class of its evidence, whether
// each era claims it or declares it absent, and which mechanism may carry each
// era's row by entailment.
type mechanism struct {
	name         string
	proposition  string
	witnesses    map[EvidenceKind]string
	dispositions map[string]string
	entailments  map[string]string
}

func (m mechanism) absent(era string) bool { return m.dispositions[era] == dispositionAbsent }

// lexicon is mechanisms.txt parsed. It is never held between reads: denoted
// materializes a fresh one from the sealed text every time, so what one caller
// got is that caller's copy and a write to it reaches nothing else.
type lexicon struct {
	ordered []mechanism
	index   map[string]int
}

func (l lexicon) named(name string) (mechanism, bool) {
	at, frozen := l.index[name]
	if !frozen {
		return mechanism{}, false
	}
	return l.ordered[at], true
}

func denoted() lexicon {
	read, err := readMechanisms()
	if err != nil {
		panic("mixed-era: " + err.Error())
	}
	return read
}

// readMechanisms reads what each mechanism name denotes, who may witness it, how
// each era disposes of it, and which row may carry which by entailment. Together
// these bind a name to one claim, one evidence site, one per-era answer, and one
// declared relation: a peer that does not quote its proposition back is refused,
// no two names may carry one proposition, a fact filed anywhere but the
// registered witness is refused, dropping a peer's absence needs the disposition
// here flipped in the same commit, and a claim carried by entailment needs the
// pair written here at matching polarity. What none of it refuses is a
// redefinition carried through consistently — rewritten here and in both era
// peers, or flipped here and in the peer that declared it — which is the point:
// that redefinition lands as a diff to this file, where a reviewer reads it,
// instead of as a silently repointed name.
func readMechanisms() (lexicon, error) {
	read := lexicon{index: map[string]int{}}
	open := -1
	for _, line := range FrozenLines(mechanismFixture) {
		keyword, rest, _ := strings.Cut(line, " ")
		rest = strings.TrimSpace(rest)
		if keyword == "mechanism" {
			if rest == "" {
				return lexicon{}, fmt.Errorf("%s opens a stanza naming no mechanism", MechanismPath)
			}
			if _, named := read.index[rest]; named {
				return lexicon{}, fmt.Errorf("%s names %q twice", MechanismPath, rest)
			}
			read.index[rest] = len(read.ordered)
			read.ordered = append(read.ordered, mechanism{
				name:         rest,
				witnesses:    map[EvidenceKind]string{},
				dispositions: map[string]string{},
				entailments:  map[string]string{},
			})
			open = len(read.ordered) - 1
			continue
		}
		if open < 0 {
			return lexicon{}, fmt.Errorf("%s carries %q before any stanza opens with \"mechanism <name>\"", MechanismPath, line)
		}
		if err := read.ordered[open].take(keyword, rest); err != nil {
			return lexicon{}, fmt.Errorf("%s freezes %q, which %w", MechanismPath, read.ordered[open].name, err)
		}
	}
	if len(read.ordered) == 0 {
		return lexicon{}, fmt.Errorf("%s freezes no mechanism at all", MechanismPath)
	}
	denoting := map[string]string{}
	for _, held := range read.ordered {
		if err := held.complete(); err != nil {
			return lexicon{}, fmt.Errorf("%s freezes %q, which %w", MechanismPath, held.name, err)
		}
		if first, taken := denoting[held.proposition]; taken {
			return lexicon{}, fmt.Errorf("%s freezes one proposition under both %q and %q, so the two names denote the same claim and either can be read as the other:\n  %s",
				MechanismPath, first, held.name, held.proposition)
		}
		denoting[held.proposition] = held.name
	}
	if err := read.entailmentsHold(); err != nil {
		return lexicon{}, err
	}
	return read, nil
}

// entailmentsHold refuses a declared entailment that carries a row this file
// disposes of as claimed, or one it disposes of the other way from its
// antecedent. An entailment redeems a claim against no artifact of its own, so
// it reaches exactly one shape: an absence that follows from another absence.
// A claimed row names a mechanism this era ships, and a mechanism that ships
// leaves artifacts — it is redeemed against one or it is not redeemed.
func (l lexicon) entailmentsHold() error {
	for _, held := range l.ordered {
		for _, era := range []string{PrecutEra, CutEra} {
			antecedent, entailed := held.entailments[era]
			if !entailed {
				continue
			}
			backing, frozen := l.named(antecedent)
			switch {
			case antecedent == held.name:
				return fmt.Errorf("%s entails %q's %s era by itself", MechanismPath, held.name, era)
			case !frozen:
				return fmt.Errorf("%s entails %q's %s era by %q, which is no mechanism here",
					MechanismPath, held.name, era, antecedent)
			case held.dispositions[era] != dispositionAbsent:
				return fmt.Errorf("%s entails %q's %s era, disposed %s, by %q: an entailment redeems a row against no artifact of its own, so it carries only an absence that follows from another absence. A %s row names a mechanism this era ships, and one that ships leaves artifacts — redeem it against one, or dispose of the era %s",
					MechanismPath, held.name, era, held.dispositions[era], antecedent, dispositionClaimed, dispositionAbsent)
			case backing.dispositions[era] != held.dispositions[era]:
				return fmt.Errorf("%s entails %q's %s era, disposed %s, by %q's, disposed %s: an entailment carries one row on another of the same polarity, so a present fact cannot redeem an absence",
					MechanismPath, held.name, era, held.dispositions[era], antecedent, backing.dispositions[era])
			}
		}
	}
	return nil
}

func (m *mechanism) take(keyword, rest string) error {
	switch keyword {
	case "proposition":
		if m.proposition != "" {
			return errors.New("freezes two propositions")
		}
		if rest == "" {
			return errors.New("freezes an empty proposition; write the exact claim the name denotes")
		}
		m.proposition = rest
	case "witness":
		token, site, split := strings.Cut(rest, " ")
		kind, site := EvidenceKind(token), strings.TrimSpace(site)
		if _, classed := artifacts[kind]; !classed || !split || site == "" {
			return fmt.Errorf("carries \"witness %s\", and a witness reads \"witness <%s> <function>\"", rest, kindTokens())
		}
		if held, reserved := m.witnesses[kind]; reserved {
			return fmt.Errorf("reserves its %s evidence for both %s and %s", kind, held, site)
		}
		m.witnesses[kind] = site
	case "era":
		name, disposition, split := strings.Cut(rest, " ")
		disposition = strings.TrimSpace(disposition)
		if name != PrecutEra && name != CutEra {
			return fmt.Errorf("disposes of a %q era, and this matrix runs %s and %s", name, PrecutEra, CutEra)
		}
		if !split || (disposition != dispositionClaimed && disposition != dispositionAbsent) {
			return fmt.Errorf("carries \"era %s\", and an era reads \"era <era> %s|%s\"", rest, dispositionClaimed, dispositionAbsent)
		}
		if held, disposed := m.dispositions[name]; disposed {
			return fmt.Errorf("disposes of the %s era both %s and %s", name, held, disposition)
		}
		m.dispositions[name] = disposition
	case "entailed-by":
		era, antecedent, split := strings.Cut(rest, " ")
		antecedent = strings.TrimSpace(antecedent)
		if era != PrecutEra && era != CutEra {
			return fmt.Errorf("entails a %q era, and this matrix runs %s and %s", era, PrecutEra, CutEra)
		}
		if !split || antecedent == "" {
			return fmt.Errorf("carries \"entailed-by %s\", and an entailment reads \"entailed-by <era> <mechanism>\"", rest)
		}
		if held, entailed := m.entailments[era]; entailed {
			return fmt.Errorf("entails its %s era by both %s and %s", era, held, antecedent)
		}
		m.entailments[era] = antecedent
	default:
		return fmt.Errorf("carries %q, which opens none of \"proposition\", \"witness\", \"era\", or \"entailed-by\"", keyword)
	}
	return nil
}

func (m mechanism) complete() error {
	if m.proposition == "" {
		return errors.New("freezes no proposition; write the exact claim the name denotes")
	}
	for _, era := range []string{PrecutEra, CutEra} {
		if m.dispositions[era] == "" {
			return fmt.Errorf("disposes of no %s era; write whether that era claims it or declares it absent", era)
		}
	}
	return nil
}

func kindTokens() string {
	tokens := make([]string, 0, len(artifacts))
	for kind := range artifacts {
		tokens = append(tokens, string(kind))
	}
	slices.Sort(tokens)
	return strings.Join(tokens, "|")
}
