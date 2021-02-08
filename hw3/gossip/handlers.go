package gossip

import (
	"net"
	"reflect"

	"golang.org/x/xerrors"
)

// ExecuteHandler executes the 'Exec' function of the registered handler that
// takes f and g as input arguments.
// f is the message to be processes, and g is the UDP address which sent the packet.
// It also return the error of Exec if any.
func (g *Gossiper) ExecuteHandler(f interface{}, f2 *net.UDPAddr) error {
	t := reflect.TypeOf(f)

	i, found := g.Handlers[t]
	if !found {
		return xerrors.Errorf("not found")
	}

	t = reflect.TypeOf(i)

	if t.Kind() != reflect.Ptr && t.Elem().Kind() != reflect.Struct {
		return xerrors.Errorf("not a pointer of struct")
	}

	m, ok := t.MethodByName("Exec")
	if !ok {
		return xerrors.Errorf("method Exec not found")
	}

	var fVal, f2Val reflect.Value

	if f == nil {
		fVal = reflect.New(t).Elem()
	} else {
		fVal = reflect.ValueOf(f)
	}

	if f2 == nil {
		f2Val = reflect.New(reflect.TypeOf(f2)).Elem()
	} else {
		f2Val = reflect.ValueOf(f2)
	}

	res := m.Func.Call([]reflect.Value{fVal, reflect.ValueOf(g), f2Val})

	if len(res) != 1 {
		return xerrors.Errorf("expected 1 return value")
	}

	err, ok := res[0].Interface().(error)
	if !ok && !res[0].IsNil() {
		return xerrors.Errorf("return is not an error")
	}

	return err
}

// RegisterHandler stores a handler that must have an Exec function in the form
// f.Exec(g, h) error
// It uses f as the key to later exec the handler.
func (g *Gossiper) RegisterHandler(f interface{}) error {
	t := reflect.TypeOf(f)

	if t.Kind() != reflect.Ptr && t.Elem().Kind() != reflect.Struct {
		return xerrors.Errorf("expected a pointer of struct")
	}

	m, ok := t.MethodByName("Exec")
	if !ok {
		return xerrors.Errorf("method 'Exec' not found")
	}

	n := m.Type.NumIn()
	if n != 3 {
		return xerrors.Errorf("expected two arguments in the 'Exec' function")
	}

	arg := m.Type.In(0) // first arg is the parent struct
	g.Handlers[arg] = f

	return nil
}
