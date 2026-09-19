package geodata

import (
	"context"
	"sync"

	"github.com/robfig/cron/v3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	commongeodata "github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
)

type Instance struct {
	assets     []*Asset
	downloader *downloader
	tasker     *cron.Cron
	reloadData func() error
	reloadMMDB func(string) error

	mu      sync.Mutex
	running bool
}

func New(ctx context.Context, config *Config) (*Instance, error) {
	if config.Cron == "" {
		return &Instance{}, nil
	}

	g := &Instance{
		assets:     config.Assets,
		reloadData: reloadGeodata,
		reloadMMDB: commongeodata.MMDBReg.ReloadAsset,
	}

	if len(g.assets) > 0 {
		var dispatcher routing.Dispatcher
		if err := core.RequireFeatures(ctx, func(d routing.Dispatcher) {
			dispatcher = d
		}); err != nil {
			return nil, errors.New("failed to get dispatcher for geodata downloader").Base(err)
		}
		g.downloader = newDownloader(ctx, dispatcher, config.Outbound)
	}

	g.tasker = cron.New(
		cron.WithChain(cron.SkipIfStillRunning(cron.DiscardLogger)),
		cron.WithLogger(cron.DiscardLogger),
	)
	if _, err := g.tasker.AddFunc(config.Cron, g.execute); err != nil {
		return nil, errors.New("invalid geodata cron").Base(err)
	}
	errors.LogInfo(ctx, "scheduled geodata reload with cron: ", config.Cron)

	return g, nil
}

func (g *Instance) execute() {
	var err error
	if g.downloader != nil {
		err = g.reloadWithUpdate()
	} else {
		err = reload()
	}
	if err != nil {
		errors.LogErrorInner(context.Background(), err, "scheduled geodata reload failed")
	}
}

func (g *Instance) reloadWithUpdate() error {
	assets := make([]*Asset, 0, len(g.assets))
	mmdbAssets := make([]*Asset, 0, len(g.assets))
	for _, asset := range g.assets {
		if commongeodata.IsMMDBAsset(asset.File) {
			mmdbAssets = append(mmdbAssets, asset)
		} else {
			assets = append(assets, asset)
		}
	}

	var errs []error
	if len(assets) > 0 {
		errs = append(errs, g.reloadGeodataWithUpdate(assets))
	}
	for _, asset := range mmdbAssets {
		errs = append(errs, g.reloadMMDBWithUpdate(asset))
	}
	return errors.Combine(errs...)
}

func (g *Instance) reloadGeodataWithUpdate(assets []*Asset) error {
	staged, err := g.downloader.download(assets)
	if err != nil {
		return err
	}
	defer clean(staged)

	tx, err := swapAll(staged)
	if err != nil {
		return err
	}

	if err := g.reloadGeodata(); err != nil {
		errors.LogErrorInner(context.Background(), err, "failed to reload geodata after downloading assets, rolling back")
		rollbackErr := tx.rollback()
		restoreErr := g.reloadGeodata()
		return errors.Combine(err, rollbackErr, restoreErr)
	}

	return tx.commit()
}

func (g *Instance) reloadMMDBWithUpdate(asset *Asset) error {
	staged, err := g.downloader.downloadOne(asset)
	if err != nil {
		return err
	}
	defer clean([]stage{staged})

	swapped, err := swapOne(staged)
	if err != nil {
		return err
	}
	if err := g.reloadMMDBAsset(asset.File); err != nil {
		errors.LogErrorInner(context.Background(), err, "failed to reload MMDB asset ", asset.File, ", rolling back")
		return errors.Combine(err, swapped.rollback())
	}
	return swapped.commit()
}

func (g *Instance) reloadGeodata() error {
	if g.reloadData != nil {
		return g.reloadData()
	}
	return reloadGeodata()
}

func (g *Instance) reloadMMDBAsset(file string) error {
	if g.reloadMMDB != nil {
		return g.reloadMMDB(file)
	}
	return commongeodata.MMDBReg.ReloadAsset(file)
}

func reload() error {
	return errors.Combine(
		reloadGeodata(),
		commongeodata.MMDBReg.ReloadAll(),
	)
}

func reloadGeodata() error {
	return errors.Combine(commongeodata.IPReg.Reload(), commongeodata.DomainReg.Reload())
}

func (g *Instance) Type() interface{} {
	return (*Instance)(nil)
}

func (g *Instance) Start() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.running {
		return nil
	}

	if g.tasker != nil {
		g.tasker.Start()
	}

	g.running = true

	return nil
}

func (g *Instance) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.running {
		return nil
	}

	if g.tasker != nil {
		<-g.tasker.Stop().Done()
	}

	g.running = false

	return nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return New(ctx, cfg.(*Config))
	}))
}
