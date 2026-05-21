package protobuf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	connectrpc "connectrpc.com/connect"
	"github.com/bufbuild/prototransform"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/warpstreamlabs/bento/public/service"
)

type MultiModuleWatcher struct {
	bsrClients map[string]*prototransform.SchemaWatcher
}

var _ prototransform.Resolver = &MultiModuleWatcher{}

func newMultiModuleWatcher(bsrModules []*service.ParsedConfig) (*MultiModuleWatcher, error) {
	if len(bsrModules) == 0 {
		return nil, errors.New("no modules provided")
	}
	multiModuleWatcher := &MultiModuleWatcher{}

	// Initialise one client for each module
	multiModuleWatcher.bsrClients = make(map[string]*prototransform.SchemaWatcher)
	for _, bsrModule := range bsrModules {
		var bsrURL string
		bsrURL, err := bsrModule.FieldString(fieldBSRUrl)
		if err != nil {
			return nil, err
		}

		var bsrAPIKey string
		if bsrAPIKey, err = bsrModule.FieldString(fieldBsrAPIKey); err != nil {
			return nil, err
		}

		var module string
		if module, err = bsrModule.FieldString(fieldBsrModule); err != nil {
			return nil, err
		}

		var version string
		if version, err = bsrModule.FieldString(fieldBsrVersion); err != nil {
			return nil, err
		}

		watcher, err := newSchemaWatcher(context.Background(), bsrURL, bsrAPIKey, module, version)
		if err != nil {
			return nil, err
		}
		multiModuleWatcher.bsrClients[module] = watcher
	}

	return multiModuleWatcher, nil
}

func newSchemaWatcher(ctx context.Context, bsrURL string, bsrAPIKey string, module string, version string) (*prototransform.SchemaWatcher, error) {
	// If no BSR url provided, extract from module
	if bsrURL == "" {
		segments := strings.Split(module, "/")
		if len(segments) != 3 {
			return nil, fmt.Errorf("could not parse module %s, expected three segments e.g. 'buf.build/exampleco/mymodule'", module)
		}
		bsrURL = "https://" + segments[0]
	}

	opts := []connectrpc.ClientOption{
		connectrpc.WithHTTPGet(),
		connectrpc.WithHTTPGetMaxURLSize(8192, true)}

	if bsrAPIKey != "" {
		opts = append(opts, connectrpc.WithInterceptors(prototransform.NewAuthInterceptor(bsrAPIKey)))
	}
	client := reflectv1beta1connect.NewFileDescriptorSetServiceClient(http.DefaultClient, bsrURL, opts...) //

	cfg := &prototransform.SchemaWatcherConfig{
		SchemaPoller: prototransform.NewSchemaPoller(
			client,
			module,
			version,
		),
		Jitter:        0.2,
		PollingPeriod: 20 * time.Second,
		Leaser:        sharedLeaser,
		Cache:         sharedCache,
	}
	watcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema watcher: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = watcher.AwaitReady(ctxWithTimeout); err != nil {
		return nil, fmt.Errorf("schema watcher never became ready: %w", err)
	}

	return watcher, nil
}

func (w *MultiModuleWatcher) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByName(field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", field)
}

func (w *MultiModuleWatcher) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByNumber(message, field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByName(message)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *MultiModuleWatcher) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByURL(url)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", url)
}

func (w *MultiModuleWatcher) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	for _, schemaWatcher := range w.bsrClients {
		enumType, err := schemaWatcher.FindEnumByName(enum)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return enumType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", enum)
}

var (
	sharedLeaser = &bsrLeaser{}
	sharedCache  = newBsrCache()
)

var (
	mu           sync.Mutex
	currentLease *bsrLease //
	weHaveLease  bool      // <- STORE IN A CACHE
)

// cache key,value id,bool

type bsrLeaser struct{}

func (blr *bsrLeaser) NewLease(ctx context.Context, s string, id []byte) prototransform.Lease {
	mu.Lock()
	defer mu.Unlock()

	fmt.Printf("NEW LEASE for sharedLeaser(%v)\n", &sharedLeaser)

	// Does someone else have the lease?
	// Yes: We return a lease that will return IsHeld() false
	// No: We return a lease that will return IsHeld() true

	if currentLease != nil {
		fmt.Printf("RETURNING EXISTING CURRENT LEASE %v\n", &currentLease)
		// we don't hold the lease now
		weHaveLease = false // <- STORE IN A CACHE
		return currentLease
	} else {
		currentLease = &bsrLease{}
		fmt.Printf("CREATING CURRENT LEASE %v\n", &currentLease)
		// we hold the lease now
		weHaveLease = true // <- STORE IN A CACHE
		return currentLease
	}
}

type bsrLease struct{}

func (bl bsrLease) IsHeld() (bool, error) {
	mu.Lock()
	defer mu.Unlock()

	// Does someone else have the lease?
	// Yes: We return false
	// No:
	// - Then do we have the lease?
	//   Yes: We return true
	//    No: We try to obtain the lease

	return weHaveLease, nil
}

func (bl bsrLease) SetCallbacks(func(), func()) {}
func (bl bsrLease) Cancel()                     {}

type bsrCache struct {
	mu sync.RWMutex
	c  map[string][]byte
}

func newBsrCache() *bsrCache {
	return &bsrCache{c: make(map[string][]byte)}
}

func (bc *bsrCache) Load(ctx context.Context, key string) ([]byte, error) {
	fmt.Printf("LOAD key: %v\n", key)
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.c[key], nil
}

func (bc *bsrCache) Save(ctx context.Context, key string, data []byte) error {
	fmt.Printf("SAVE key: %v\n", key)
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.c[key] = data
	return nil
}
