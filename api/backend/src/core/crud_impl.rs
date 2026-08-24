macro_rules! make_create_handler {
    ($fn_name:ident, $full_ty:ty, $primary_ty:ty, $update_ty:ty, $table:expr) => {
        async fn $fn_name(
            axum::extract::State(state): axum::extract::State<crate::AppState>,
            axum::Json(payload): axum::Json<$full_ty>,
        ) -> impl axum::response::IntoResponse {
            let crud = core::crud::CRUD::<$full_ty, $primary_ty, $update_ty>::new(
                state.db.clone(),
                $table.to_string(),
            );

            match crud.create(&payload).await {
                Ok(_) => "Created".into_response(),
                Err(err) => (
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to create: {}", err),
                )
                    .into_response(),
            }
        }
    };
}

macro_rules! make_read_handler {
    ($fn_name:ident, $full_ty:ty, $primary_ty:ty, $update_ty:ty, $table:expr) => {
        async fn $fn_name(
            axum::extract::State(state): axum::extract::State<crate::AppState>,
            axum::extract::Query(pk): axum::extract::Query<$primary_ty>,
        ) -> impl axum::response::IntoResponse {
            let crud = core::crud::CRUD::<$full_ty, $primary_ty, $update_ty>::new(
                state.db.clone(),
                $table.to_string(),
            );

            match crud.read(&pk).await {
                Ok(Some(obj)) => axum::Json(obj).into_response(), // you can return the object here
                Ok(None) => (http::StatusCode::NOT_FOUND, "Item not found".to_string()).into_response(),
                Err(err) => (http::StatusCode::NOT_FOUND, format!("Not found: {}", err)).into_response(),
            }
        }
    };
}

macro_rules! make_read_all_handler {
    ($fn_name:ident, $full_ty:ty, $primary_ty:ty, $update_ty:ty, $table:expr) => {
        async fn $fn_name(axum::extract::State(state): axum::extract::State<crate::AppState>) -> impl axum::response::IntoResponse {
            let crud = core::crud::CRUD::<$full_ty, $primary_ty, $update_ty>::new(
                state.db.clone(),
                $table.to_string(),
            );

            match crud.read_all().await {
                Ok(Some(obj)) => axum::Json(obj).into_response(), // you can return the object here
                Ok(None) => (
                    http::StatusCode::NOT_FOUND,
                    format!("No entries for table found: {}", $table),
                )
                    .into_response(),
                Err(err) => (http::StatusCode::NOT_FOUND, format!("Not found: {}", err)).into_response(),
            }
        }
    };
}

macro_rules! make_update_handler {
    ($fn_name:ident, $full_ty:ty, $primary_ty:ty, $update_ty:ty, $table:expr) => {
        async fn $fn_name(
            axum::extract::State(state): axum::extract::State<crate::AppState>,
            axum::Json((pk, update)): axum::Json<($primary_ty, $update_ty)>,
        ) -> impl axum::response::IntoResponse {
            let crud = core::crud::CRUD::<$full_ty, $primary_ty, $update_ty>::new(
                state.db.clone(),
                $table.to_string(),
            );

            match crud.update(&pk, &update).await {
                Ok(_) => "Updated".into_response(),
                Err(err) => (
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to update: {}", err),
                )
                    .into_response(),
            }
        }
    };
}

macro_rules! make_delete_handler {
    ($fn_name:ident, $full_ty:ty, $primary_ty:ty, $update_ty:ty, $table:expr) => {
        async fn $fn_name(
            axum::extract::State(state): axum::extract::State<crate::AppState>,
            axum::Json(pk): axum::Json<$primary_ty>,
        ) -> impl axum::response::IntoResponse {
            let crud = core::crud::CRUD::<$full_ty, $primary_ty, $update_ty>::new(
                state.db.clone(),
                $table.to_string(),
            );

            match crud.delete(&pk).await {
                Ok(_) => "Deleted".into_response(),
                Err(err) => (
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to delete: {}", err),
                )
                    .into_response(),
            }
        }
    };
}

pub(crate) use make_create_handler;
pub(crate) use make_delete_handler;
pub(crate) use make_read_all_handler;
pub(crate) use make_read_handler;
pub(crate) use make_update_handler;

macro_rules! make_crud_handlers {
    (
        $create_name:ident,
        $read_name: ident,
        $read_all_name: ident,
        $update_name: ident,
        $delete_name: ident,
        $full_ty:ty, 
        $primary_ty:ty, 
        $update_ty:ty, 
        $table:expr
     ) => {
        crate::core::crud_impl::make_create_handler!(
            $create_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
        crate::core::crud_impl::make_read_handler!(
            $read_name, 
            $full_ty, 
            $primary_ty, 
            $update_ty, 
            $table
        );
        crate::core::crud_impl::make_read_all_handler!(
            $read_all_name, 
            $full_ty, 
            $primary_ty, 
            $update_ty, 
            $table
        );
        crate::core::crud_impl::make_update_handler!(
            $update_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
        crate::core::crud_impl::make_delete_handler!(
            $delete_name,
            $full_ty,
            $primary_ty,
            $update_ty,
            $table
        );
    };
}

pub(crate) use make_crud_handlers;
