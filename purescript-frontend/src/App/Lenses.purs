module App.Lenses where

import Data.Lens (Lens')
import Data.Lens.Record (prop)
import Data.Symbol (SProxy(..))

_id :: forall a r. Lens' { id :: a | r } a
_id = prop (SProxy :: SProxy "id")

_type_ :: forall a r. Lens' { type_ :: a | r } a
_type_ = prop (SProxy :: SProxy "type_")

_editId :: forall a r. Lens' { editId :: a | r } a
_editId = prop (SProxy :: SProxy "editId")

_editPuck :: forall a r. Lens' { editPuck :: a | r } a
_editPuck = prop (SProxy :: SProxy "editPuck")

_rows :: forall a r. Lens' { rows :: a | r } a
_rows = prop (SProxy :: SProxy "rows")

_editData :: forall a r. Lens' { editData :: a | r } a
_editData = prop (SProxy :: SProxy "editData")

_requestResult :: forall a r. Lens' { requestResult :: a | r } a
_requestResult = prop (SProxy :: SProxy "requestResult")

_modalTarget :: forall a r. Lens' { modalTarget :: a | r } a
_modalTarget = prop (SProxy :: SProxy "modalTarget")

_dewarPosition :: forall a r. Lens' { dewarPosition :: a | r } a
_dewarPosition = prop (SProxy :: SProxy "dewarPosition")

_inputRow :: forall a r. Lens' { inputRow :: a | r } a
_inputRow = prop (SProxy :: SProxy "inputRow")

_puckId :: forall a r. Lens' { puckId :: a | r } a
_puckId = prop (SProxy :: SProxy "puckId")
