use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys},
    },
    implement_all_crud_methods,
};

pub struct CancelledOrdersCRUD<'a> {
    pub(super) crud:
        CRUD<'a, CancelledOrdersFullKeys, CancelledOrdersPrimaryKeys, CancelledOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    CancelledOrdersFullKeys,
    CancelledOrdersPrimaryKeys,
    CancelledOrdersUpdateKeys,
    CancelledOrdersCRUD
);

#[hotpath::measure_all]
impl<'a> CancelledOrdersCRUD<'a> {
    pub fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                CancelledOrdersFullKeys,
                CancelledOrdersPrimaryKeys,
                CancelledOrdersUpdateKeys,
            >::new(exec, "logs.cancelled_orders"),
        }
    }
}
