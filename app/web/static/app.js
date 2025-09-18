// app/web/static/app.js
document.addEventListener('DOMContentLoaded', () => {
  const $ = (id) => document.getElementById(id);

  // ───────────────── password modal (новая/старая разметка) ─────────────────
  const btnOpen   = $('btnChangePwd');
  const modal     = $('pwdModal');     // либо <dialog>, либо .modal-backdrop
  const msg       = $('pwdMsg');
  const btnClose  = $('pwdClose');
  const btnCancel = $('pwdCancel');
  const btnSubmit = $('pwdSubmit');
  const formOld   = $('pwdForm');      // старый вариант с <form id="pwdForm">

  const hasNewModal = !!(modal && modal.getAttribute); // див с aria-hidden
  const hasOldModal = !!(modal && typeof modal.showModal === 'function'); // <dialog>

  function openModal() {
    if (!modal) return;
    if (hasOldModal) modal.showModal();
    else modal.setAttribute('aria-hidden', 'false');
    if (msg) msg.textContent = '';
    // очистка полей в новой разметке
    const f = {
      old: $('pwdOld'),
      neu: $('pwdNew'),
      rep: $('pwdConfirm'),
    };
    if (f.old) f.old.value = '';
    if (f.neu) f.neu.value = '';
    if (f.rep) f.rep.value = '';
    // и в старой разметке очистим форму
    if (formOld && typeof formOld.reset === 'function') formOld.reset();
  }
  function closeModal() {
    if (!modal) return;
    if (hasOldModal) modal.close();
    else modal.setAttribute('aria-hidden', 'true');
  }

  if (btnOpen && modal) {
    btnOpen.addEventListener('click', (e) => { e.preventDefault(); openModal(); });
  }
  if (btnClose)  btnClose.addEventListener('click', closeModal);
  if (btnCancel) btnCancel.addEventListener('click', closeModal);
  if (modal && !hasOldModal) {
    // клик по фону в новой разметке закрывает модалку
    modal.addEventListener('click', (e) => { if (e.target === modal) closeModal(); });
  }

  async function doChangePassword(old_password, new_password, confirm_password) {
    if (!msg) return;
    if (!old_password || !new_password || !confirm_password) {
      msg.textContent = 'Заполните все поля'; return;
    }
    if (new_password !== confirm_password) {
      msg.textContent = 'Подтверждение не совпадает'; return;
    }
    try {
      const r = await fetch('/api/auth/change_password', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ old_password, new_password, confirm_new_password: confirm_password })
      });
      if (r.ok) { msg.textContent = 'Пароль изменён'; setTimeout(closeModal, 700); }
      else      { msg.textContent = (await r.text()) || 'Ошибка смены пароля'; }
    } catch (e) {
      msg.textContent = 'Сеть недоступна';
    }
  }

  if (btnSubmit) {
    // новая разметка: берём поля по id
    btnSubmit.addEventListener('click', () => {
      const oldp = $('pwdOld')?.value || '';
      const newp = $('pwdNew')?.value || '';
      const conf = $('pwdConfirm')?.value || '';
      doChangePassword(oldp, newp, conf);
    });
  } else if (formOld) {
    // старая разметка: работаем с <form id="pwdForm">
    const cancel = $('pwdCancel');
    if (cancel) cancel.addEventListener('click', closeModal);
    formOld.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(formOld);
      doChangePassword(
        fd.get('old_password') || '',
        fd.get('new_password') || '',
        fd.get('confirm_password') || ''
      );
    });
  }

  // ───────────────── поиск на странице "Текущие": чистим автоподстановку ─────────────────
  const search = $('fltText');
  if (search) {
    // метим ручной ввод, чтобы не стирать его
    search.addEventListener('input', () => { search.dataset.userTyped = '1'; }, { once: true });

    const clearAutofill = () => {
      // если браузер подставил что-то (часто "user") и пользователь не трогал — очищаем
      if (search && search.value && !search.dataset.userTyped) {
        // иногда менеджеры паролей ставят name="username". У нас name="search",
        // но на всякий случай чистим "user" и похожее.
        if (/^user$/i.test(search.value) || search.autocomplete !== 'off') {
          search.value = '';
          search.dispatchEvent(new Event('input', { bubbles: true }));
        }
      }
    };
    // два шага: сразу и чуть позже — перехватываем медленное автозаполнение
    setTimeout(clearAutofill, 50);
    setTimeout(clearAutofill, 350);
  }
});
