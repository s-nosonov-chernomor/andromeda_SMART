// app.js
document.addEventListener('DOMContentLoaded', ()=>{
  const pwdBtn = document.getElementById('btnChangePwd');
  const dlg = document.getElementById('pwdModal');
  const frm = document.getElementById('pwdForm');
  const msg = document.getElementById('pwdMsg');
  const cancel = document.getElementById('pwdCancel');
  if(pwdBtn){
    pwdBtn.onclick = ()=>{ dlg.showModal(); msg.textContent=''; frm.reset(); };
    cancel.onclick = ()=> dlg.close();
    frm.onsubmit = async (e)=>{
      e.preventDefault();
      const fd = new FormData(frm);
      const res = await fetch('/api/account/change_password',{
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({
          old_password: fd.get('old_password'),
          new_password: fd.get('new_password'),
          confirm_password: fd.get('confirm_password')
        })
      });
      if(res.ok){ msg.textContent='Пароль изменён'; setTimeout(()=>dlg.close(),800); }
      else { msg.textContent = (await res.text()) || 'Ошибка'; }
    };
  }
});
