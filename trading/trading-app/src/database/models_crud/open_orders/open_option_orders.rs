use crate::{
    database::{
        crud::CRUD,
        db::Exec,
        models::{
            OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys,
        },
    },
    implement_all_crud_methods,
};

pub struct OpenOptionOrdersCRUD<'a> {
    pub(super) crud:
        CRUD<'a, OpenOptionOrdersFullKeys, OpenOptionOrdersPrimaryKeys, OpenOptionOrdersUpdateKeys>,
}

implement_all_crud_methods!(
    crud,
    OpenOptionOrdersFullKeys,
    OpenOptionOrdersPrimaryKeys,
    OpenOptionOrdersUpdateKeys,
    OpenOptionOrdersCRUD
);

#[hotpath::measure_all]
impl<'a> OpenOptionOrdersCRUD<'a> {
    pub(crate) fn new(exec: Exec<'a>) -> Self {
        Self {
            crud: CRUD::<
                OpenOptionOrdersFullKeys,
                OpenOptionOrdersPrimaryKeys,
                OpenOptionOrdersUpdateKeys,
            >::new(exec, "trading.open_option_orders"),
        }
    }
}
